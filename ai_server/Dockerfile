FROM nvidia/cuda:11.8.0-cudnn8-devel-ubuntu22.04

# Remove any third-party apt sources to avoid issues with expiring keys.
RUN rm -f /etc/apt/sources.list.d/*.list
RUN apt-get update --fix-missing 
RUN apt-get update

RUN DEBIAN_FRONTEND=noninteractive TZ=Asia/Seoul apt-get install -y tzdata

# Install some basic utilities
RUN apt-get install -y \
    curl \
    ca-certificates \
    sudo \
    git \
    bzip2 \
    libx11-6 \
    python3 \
    python3-pip \
    libglfw3-dev \
    libgles2-mesa-dev \
    libglib2.0-0 \
    fontconfig \
    unzip \
    wget \
    # FFmpeg 소스 빌드 의존성
    nasm \
    yasm \
    pkg-config \
    libx264-dev \
    libx265-dev \
    libnuma-dev \
    libdrm-dev \
 && rm -rf /var/lib/apt/lists/*

# ── FFmpeg with NVIDIA NVDEC/NVENC 소스 빌드 ──
# 1) nv-codec-headers 설치 (NVIDIA Video Codec SDK 헤더)
RUN git clone https://github.com/FFmpeg/nv-codec-headers.git /tmp/nv-codec-headers \
 && cd /tmp/nv-codec-headers \
 && git checkout n12.1.14.0 \
 && make install \
 && rm -rf /tmp/nv-codec-headers

# 2) FFmpeg 소스 빌드 (h264_cuvid, h264_nvenc 포함)
# 2) FFmpeg 소스 빌드
RUN git clone --depth 1 --branch n6.1.2 https://github.com/FFmpeg/FFmpeg.git /tmp/ffmpeg \
 && cd /tmp/ffmpeg \
 && ./configure \
    --enable-gpl \
    --enable-nonfree \
    --enable-cuda \
    --enable-cuda-nvcc \
    --enable-nvenc \
    --enable-nvdec \
    --enable-cuvid \
    --enable-libnpp \
    --enable-libx264 \
    --enable-libx265 \
    --extra-cflags="-I/usr/local/cuda/include" \
    --extra-ldflags="-L/usr/local/cuda/lib64" \
 && make -j$(nproc) \
 && make install \
 && rm -rf /tmp/ffmpeg

# 소스 빌드 FFmpeg 우선 사용 + 심볼릭 링크
ENV PATH="/usr/local/bin:${PATH}"
RUN ln -sf /usr/local/bin/ffmpeg /usr/bin/ffmpeg \
 && ln -sf /usr/local/bin/ffprobe /usr/bin/ffprobe

RUN curl -o nanumfont.zip http://cdn.naver.com/naver/NanumFont/fontfiles/NanumFont_TTF_ALL.zip
RUN mkdir -p /usr/share/fonts/truetype/nanum
RUN unzip -d /usr/share/fonts/truetype/nanum nanumfont.zip
RUN fc-cache -f -v


# Create a working directory
WORKDIR /root

# ENV CUDA_HOME="/usr/local/cuda-11.8"
# ENV PATH="/usr/local/cuda-11.8/bin:$PATH"
# ENV LD_LIBRARY_PATH="/usr/local/cuda-11.8/lib64:$LD_LIBRARY_PATH"
# ENV TORCH_CUDA_ARCH_LIST="8.9+PTX"
# ENV FORCE_CUDA="1"
# ENV GRANT_SUDO=yes
# ENV JUPYTER_ALLOW_INSECURE_WRITES=true
# ENV TCNN_CUDA_ARCHITCTURES=86

# 주피터 폴더 권한 오류 권한 허용
ENV JUPYTER_ALLOW_INSECURE_WRITES=true

RUN pip3 install --upgrade pip

#4090
RUN pip3 install torch==2.3.0 torchvision==0.18.0 torchaudio==2.3.0 --index-url https://download.pytorch.org/whl/cu118

COPY . .

RUN pip3 install python-dotenv==1.0.1

ENTRYPOINT ["tail", "-f", "/dev/null"]

