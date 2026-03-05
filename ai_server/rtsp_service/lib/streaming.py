import os
import sys
import subprocess
import threading
import time
import numpy as np


class FrameCache:
    """스레드 안전 프레임 캐시."""
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._ts = {}           # 카메라별 마지막 프레임 수신 시각

    def put(self, cam_id, frame):
        with self._lock:
            self._data[cam_id] = frame
            self._ts[cam_id] = time.time()

    def get(self, cam_id):
        with self._lock:
            return self._data.get(cam_id)

    def get_age(self, cam_id):
        """마지막 프레임으로부터 경과 시간(초). 프레임 없으면 inf."""
        with self._lock:
            ts = self._ts.get(cam_id)
            return time.time() - ts if ts else float('inf')


class Grabber(threading.Thread):
    """RTSP 그랩 스레드 (FFmpeg GPU→SW 폴백)."""

    # 소스 빌드 FFmpeg 경로 (Debian 시스템 ffmpeg 대신 사용)
    FFMPEG_PATH = '/usr/local/bin/ffmpeg'

    def __init__(self, cam_id, url, cache, stop_event, width, height):
        super().__init__(daemon=True)
        self.cam_id = cam_id
        self.url    = url
        self.cache  = cache
        self.stop   = stop_event
        self.width  = width
        self.height = height
        self.frame_size = width * height * 3
        self.proc   = None
        self.use_nvdec = True   # GPU NVDEC 디코딩 (SW 폴백 자동)

    def _build_command(self):
        ffmpeg_bin = self.FFMPEG_PATH if os.path.isfile(self.FFMPEG_PATH) else 'ffmpeg'
        cmd = [ffmpeg_bin]

        # if self.use_nvdec:
        #     cmd += ['-hwaccel', 'cuda', '-c:v', 'h264_cuvid']

        cmd += [
            '-rtsp_transport', 'tcp',

            # 안정화 옵션
            '-analyzeduration', '1000000',   # 1MB 분석
            '-probesize', '1000000',

            '-fflags', '+genpts',            # PTS 생성
            # '-hwaccel', 'cuda',
            # '-c:v', 'h264_cuvid'
            '-i', self.url,

            '-f', 'rawvideo',
            '-pix_fmt', 'bgr24',
            '-an',
            '-v', 'error',
            '-'
        ]
        return cmd

    def _stderr_reader(self):
        """stderr를 별도 스레드에서 읽어 로그 출력 (deadlock 방지)"""
        try:
            for line in self.proc.stderr:
                msg = line.decode('utf-8', errors='replace').strip()
                if msg:
                    print(f"[{self.cam_id}] FFmpeg: {msg}", flush=True)
        except Exception:
            pass

    def _open(self):
        if self.proc:
            try:
                self.proc.kill()
                self.proc.wait(timeout=3)
            except Exception:
                pass
        cmd = self._build_command()
        mode = "NVDEC GPU" if self.use_nvdec else "SW CPU"
        print(f"[{self.cam_id}] FFmpeg {mode} 연결 시도: {self.url} (bin: {cmd[0]})", flush=True)
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )
        # stderr 읽기 스레드 (FFmpeg 에러 로그 출력용)
        t = threading.Thread(target=self._stderr_reader, daemon=True)
        t.start()
        print(f"[{self.cam_id}] FFmpeg {mode} 디코더 시작 (PID: {self.proc.pid})", flush=True)

    def run(self):
        self._open()
        fail_count = 0
        while not self.stop.is_set():
            try:
                raw = b''
                while len(raw) < self.frame_size:
                    chunk = self.proc.stdout.read(self.frame_size - len(raw))
                    if not chunk:
                        break
                    raw += chunk
                if len(raw) != self.frame_size:
                    fail_count += 1
                    rc = self.proc.poll()
                    if rc is not None:
                        print(f"[{self.cam_id}] FFmpeg 프로세스 종료 (exit code: {rc})", file=sys.stderr, flush=True)
                    # NVDEC 3회 실패 → SW 폴백
                    if self.use_nvdec and fail_count >= 3:
                        print(f"[{self.cam_id}] NVDEC {fail_count}회 실패 → SW 디코딩으로 전환", file=sys.stderr, flush=True)
                        self.use_nvdec = False
                        fail_count = 0
                    else:
                        print(f"[{self.cam_id}] 읽기 실패({fail_count}) → {2 * fail_count}초 후 재연결", file=sys.stderr, flush=True)
                    time.sleep(min(2 * fail_count, 10))  # 점진적 대기 (최대 10초)
                    self._open()
                    continue
                fail_count = 0
                frame = np.frombuffer(raw, dtype=np.uint8).reshape(
                    (self.height, self.width, 3))
                self.cache.put(self.cam_id, frame)
            except Exception as e:
                print(f"[{self.cam_id}] Grabber 오류: {e}", file=sys.stderr, flush=True)
                time.sleep(1)
                self._open()

        if self.proc:
            try:
                self.proc.kill()
                self.proc.wait(timeout=3)
            except Exception:
                pass


def rtmp_stream_func(result_queue, rtmp_url, width, height, set_fps):
    """RTMP 스트림 전송 프로세스."""
    print("[rtmp_stream_func] rtmp_url =", rtmp_url)
    print("[rtmp_stream_func] size =", width, height)

    ffmpeg_bin = Grabber.FFMPEG_PATH if os.path.isfile(Grabber.FFMPEG_PATH) else 'ffmpeg'
    command = [
        ffmpeg_bin,
        '-y',
        '-f', 'rawvideo',
        '-vcodec', 'rawvideo',
        '-pix_fmt', 'bgr24',
        '-s', "{}x{}".format(width, height),
        '-r', str(set_fps),
        '-i', '-',
        '-c:v', 'h264_nvenc',
        '-preset', 'fast',          # p4 대신 fast (안정적)
        '-profile:v', 'high',
        '-pix_fmt', 'yuv420p',

        '-b:v', '2500k',
        '-maxrate', '2500k',
        '-bufsize', '2500k',

        '-g', str(set_fps * 2),     # GOP = 2초
        '-bf', '0',                 # B-frame 제거 (중요!!)

        '-f', 'flv',
        rtmp_url
    ]

    p = subprocess.Popen(command, stdin=subprocess.PIPE)
    last_frame = None
    frame_interval_sec = 1.0 / max(1, set_fps)

    while True:
        if result_queue.qsize() == 0:
            if last_frame is None:
                time.sleep(0.01)
                continue
            frame = last_frame
            time.sleep(frame_interval_sec)
        else:
            frame = result_queue.get()
            last_frame = frame

        if frame is None:
            continue

        try:
            p.stdin.write(frame.tobytes())
        except (BrokenPipeError, IOError):
            p.stdin.close()
            p.wait()
            p = subprocess.Popen(command, stdin=subprocess.PIPE)
            time.sleep(1)
