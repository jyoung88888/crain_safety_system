import cv2
import numpy as np
import subprocess
import multiprocessing as mp
import time
from ultralytics import YOLO
from ortools.sat.python import cp_model
import argparse
import os
import psycopg2 # type: ignore
import json
import queue
import zmq
import base64
import torch
from datetime import timedelta, datetime
from blueprints.lib.public_func import get_sim_input_df, insert_sim_jobs, insert_sim_macs, insert_sim_list, update_sim_master, get_sim_master_created_at
import math
import pandas as pd
import random



def main(sim_id):

    print("sim_id: ", sim_id)
    #시뮬레이션 구동

    # dt_sim_master의 created_at 값을 start_date로 사용
    start_date = get_sim_master_created_at(sim_id).strftime('%Y-%m-%d')
    # start_date = '2025-06-01'

    #입력 값 받기
    job_list = get_sim_input_df(sim_id)
    # sun_no 컬럼 추가 (index + 1)
    job_list['sun_no'] = job_list.index + 1

    # 컬럼명 변경
    job_list = job_list.rename(columns={
        'ordnum': '수주번호',
        'ordseq': '수주순번',
        'rate': '작업진행률(%)',
        'location': '위치',
        'detail_location': '작업장',
        'dlvdt': '납기일',
        'enddt': '종료일자',
        'hullno': '호선명',
        'work_time': '작업시간',
        'free_time': '여유시간',
        'sun_no': '순번'
    })

    print(job_list)

    # '순번'과 '작업장' 컬럼만 추출
    # machine_list = job_list[['순번', '작업장']].copy()

    # 작업장이 None이 아닌 행만 남기고 순번, 작업장만 선택
    machine_list = job_list[job_list['작업장'].notna()][['순번', '작업장']].copy()

    # 순번을 1부터 재지정
    machine_list['순번'] = range(1, len(machine_list) + 1)

    print(machine_list)

    try:
        # Check if start_date is a pandas Timestamp object
        if hasattr(start_date, 'strftime'):
            # If it's a datetime-like object (including pandas Timestamp), use strftime
            start_point = datetime.strptime(start_date.strftime('%Y-%m-%d'), '%Y-%m-%d')
        else:
            # If it's a string, try to parse it
            start_point = datetime.strptime(start_date, '%Y-%m-%d')
    except Exception as e:
        print(e)
        # If all else fails, try to split the string (if it's a string)
        if isinstance(start_date, str):
            start_point = datetime.strptime(start_date.split()[0], '%Y-%m-%d')
        else:
            # If it's not a string and previous methods failed, raise the exception
            raise ValueError(f"Unable to parse date from {type(start_date)}: {start_date}")

    machine_list = pd.DataFrame(machine_list)

    df_data = pd.DataFrame(job_list)
    df_data.index = [f'{i}번' for i in df_data['순번']]
    df_data1 = pd.DataFrame(machine_list)
    df_data1.index = [f'{i}번' for i in df_data1['순번']]

    # 작업 및 Cell이름
    job_names = df_data['호선명'].values
    machine_names = df_data1['작업장'].values

    # 작업 및 Cell 개수
    num_jobs = len(df_data)
    print(num_jobs)
    num_machines = len(machine_names)
    random.seed(42)  # For reproducibility

    # 작업준비시간 (Setup Time) fn_get_sim_result함수와 동일하게 설정해야됩
    setup_time = 1

    # due dates
    df_dj = df_data['납기일'].dropna(axis=0)
    due_dates = ["{}-{}-{}".format(str(i)[:4],str(i)[4:6],str(i)[6:]) for i in df_dj]
    due_dates_datetime = pd.to_datetime(due_dates)

    # start point에 맞춰진 납기일 (납기일이 start point로부터 얼마나 남았는지 나타냄)
    df_due_dates = pd.DataFrame(due_dates_datetime - start_point)
    due_dates_list = df_due_dates[0].dt.days.to_list()

    # processing times
    processing_times = df_data['작업시간'].to_list()
    # Replace None values with 0
    processing_times = [0 if pt is None else pt for pt in processing_times]
    print(processing_times)
    # slack times
    slack_times = df_data['여유시간'].to_list()
    # Replace None values with 0
    slack_times = [0 if st is None else st for st in slack_times]
    print(slack_times)
    # ready time = 납기일-(작업시간+slack times)
    # Since we've already handled None values in processing_times and slack_times,
    # we can use the original list comprehension
    ready_times = [due_dates_list[i]-(processing_times[i] + slack_times[i]) for i in range(num_jobs)]

    # 작업시간 계산
    ## 초기작업의 작업진행률
    progress_rates = df_data['작업진행률(%)'].to_list()
    ## 초기작업의 인덱스
    progressing_list = list(filter(lambda i: progress_rates[i] is not None and progress_rates[i] != 0, range(len(progress_rates))))
    ## 작업진행률에 따라 초기작업 작업시간 변경 (= pj * (1-작업진행률))
    for idx in progressing_list:
        processing_times[idx] = processing_times[idx] * (1-progress_rates[idx]/100)
        print(processing_times[idx])
    ## 작업이 시작되지 않은 작업에 setup time 1일 추가

    processing_times = [pj + setup_time if progress_rates[idx]==0 else pj for idx, pj in enumerate(processing_times)]


    # 작업진행률에 따른 실수형태의 남은 작업시간 처리 → 올림
    ## 모든 작업 올림 (소수점 제거)  ------>  직후 작업의 실제 setup시간은 1일보다 크다. ex) 1.77일
    ## 단, 초기에 작업중인 작업물은 마감기한이 2일 이상은 남아있어야 한다. 1일도 안남은 경우 infeasible한 해 도출
    processing_times = list(map(math.ceil, processing_times))

    # Initialize the model (이후 제약조건과 목표를 추가하여 최적의 해를 찾게 된다.)
    model = cp_model.CpModel()
    time_limit = 5  # seconds

    # Decision variables
    job_assignment = {}     # 각 작업이 어느 셀에 할당되었는지 나타내는 binary변수
    start_times = {}        # 작업 시작시간
    end_times = {}          # 작업 끝나는 시간
    interval_vars = {}      # 각 작업의 기간을 정의하는 변수
    existing_task = {}      # 고정된 작업을 정의하는 변수

    # 작업할당변수 및 interval변수 생성
    for j in range(num_jobs):
        if j not in progressing_list:
            # 작업시작시간, 작업완료시간
            start_times[j] = model.NewIntVar(ready_times[j], due_dates_list[j] - processing_times[j], f'start_{j}')
            end_times[j] = model.NewIntVar(ready_times[j] + processing_times[j], due_dates_list[j], f'end_{j}')
        else:
            # 초기 작업의 경우 시작시점, 완료시점 고정
            start_times[j] = 0
            end_times[j] = 0 + processing_times[j]

        for m in range(num_machines):
            # 할당변수
            job_assignment[(j, m)] = model.NewBoolVar(f'job_{j}_machine_{m}')

            # interval변수 : job_assignment[(j, m)]가 1의 값을 가질 때만 기계에 할당된 것으로 간주한다.
            interval_vars[(j, m)] = model.NewOptionalIntervalVar(start_times[j], processing_times[j], end_times[j], job_assignment[(j, m)],
                                                        f'interval_{j}_machine_{m}')

    # cell사용여부 변수
    machine_used = {}
    for m in range(num_machines):
        machine_used[m] = model.NewBoolVar(f'machine_{m}_used')

    # cell사용여부를 나타내는 제약식 (한 작업이라도 할당되면 'cell사용여부 변수 = 1')
    for m in range(num_machines):
        for j in range(num_jobs):
            model.Add(machine_used[m] == 1).OnlyEnforceIf(job_assignment[(j, m)])

    # cell이 정해진 작업, 해당 cell에 고정 → 'job_fixed_machine' 리스트에 각 작업의 고정 Cell 저장
    machine_name_to_index = {name: idx for idx, name in enumerate(machine_names)}
    job_fixed_machine = []
    for work_cell in df_data['작업장']:
        if pd.isna(work_cell) or work_cell.strip() == '':
            job_fixed_machine.append(None)
        else:
            job_fixed_machine.append(machine_name_to_index.get(work_cell.strip(), None))

    # cell이 정해진 작업 → 해당 cell 외의 모든 cell에 대한 할당변수값을 0으로 고정하는 제약식
    for j in range(num_jobs):
        fixed_m = job_fixed_machine[j]
        if fixed_m is not None:
            for m in range(num_machines):
                if m != fixed_m:
                    model.Add(job_assignment[(j, m)] == 0)

    ''' 같은 cell 내 overlap금지 제약 '''
    # 각 Cell마다 할당된 작업(interval변수)을 추가한 후, 해당 Cell 안에 있는 작업들이 겹치지 않게 제약 설정
    for m in range(num_machines):
        assigned_intervals = []
        for j in range(num_jobs):
            # 각 작업을 스케줄표에 배치
            assigned_intervals.append(interval_vars[(j, m)])
        # 배치된 작업들끼리 겹치지 않게 만드는 제약식
        model.AddNoOverlap(assigned_intervals)


    for j in range(num_jobs):
        # 각 작업은 단 하나의 기계에만 할당된다.
        model.Add(sum(job_assignment[(j, m)] for m in range(num_machines)) == 1)
        if j not in progressing_list:
            for m in range(num_machines):
                # 작업시간은 무조건 0시점 이후에 시작
                model.Add(start_times[j] >= 0)


    # 전체 사용된 기계의 수를 최소화
    model.Minimize(sum(machine_used[m] for m in range(num_machines)))

    # Solving with time limit
    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = 8   #os.cpu_count()  # 또는 예: 4
    solver.parameters.max_time_in_seconds = time_limit
    status = solver.Solve(model)
    print(status)

    print(cp_model.FEASIBLE)
    print(cp_model.OPTIMAL)
    ##### ----- setup time을 고려하지 않은 시각화 ----- #####

    print(f"\nSolverssStatus: {solver.StatusName(status)}")
    #여기!!print("s:",solver.StatusName(status))
    update_sim_master(sim_id=sim_id, free_mac=None, relaxed_time=None, status=solver.StatusName(status))

    if status in [cp_model.FEASIBLE, cp_model.OPTIMAL]:
        print("📋 Job Assignment Result:")
        schedule = []
        for j in range(num_jobs):
            for m in range(num_machines):
                if solver.Value(job_assignment[(j, m)]):    # 작업 j가 Cell m에 할당되었을 때,
                    s = solver.Value(start_times[j])
                    e = solver.Value(end_times[j])
                    schedule.append([j, m, s, e, ready_times[j], due_dates_list[j]])
                    start_dt = start_point + timedelta(days=s)
                    enddt = start_point + timedelta(days=e)
                    # print(f"Job {job_names[j]} ➤ Machine {m + 1:>2} | Start: {s:>3} ({start_dt.strftime('%Y-%m-%d')}), End: {e:>3} ({enddt.strftime('%Y-%m-%d')})")
                    print(f"Job {job_names[j]} ➤ Machine {machine_names[int(m)]} | Start: {s:>3} ({start_dt.strftime('%Y-%m-%d')}), End: {e:>3} ({enddt.strftime('%Y-%m-%d')})")
                    break
        df = pd.DataFrame(schedule, columns=["Job", "Machine", "Start", "End", "Ready Time", "Due Date"])
        df["Start Date"] = df["Start"].apply(lambda x: start_point + timedelta(days=x))
        df["End Date"] = df["End"].apply(lambda x: start_point + timedelta(days=x))
        df["Ready Date"] = df["Ready Time"].apply(lambda x: start_point + timedelta(days=x))
        df["Due Date Date"] = df["Due Date"].apply(lambda x: start_point + timedelta(days=x))
        df["Order"] = df.groupby("Machine").cumcount()      # Cell 내에서의 순서

        # 시각화용 y축: Machine 인덱스를 숫자로 정리
        machine_id_to_name = {i: name for i, name in enumerate(machine_names)}
        df["Machine ID"] = df["Machine"]
        print(df)

        last_end_date = df.iloc[-1]['End Date']
        last_Due_Date_Date = df.iloc[-1]['Due Date Date']

        # 문자열을 datetime 객체로 변환
        d1 = datetime.strptime(str(last_end_date.date()), "%Y-%m-%d")
        d2 = datetime.strptime(str(last_Due_Date_Date.date()), "%Y-%m-%d")

        relaxed_time = (d2 - d1).days
        # print('relaxed_time')
        # print(relaxed_time)

        # print('Machine')
        # print(df['Machine'].values)

        # 전체 기계 집합에서 사용된 기계를 제거
        free_mac = num_machines - len(set(df['Machine'].values))

        #dt_sim_job 내용 추가
        insert_sim_jobs(sim_id=sim_id, job_names=job_names)

        #sim_list 내용 추가 (결과는 항상 저장)
        insert_sim_list(sim_id=sim_id, df=df)

        #dt_sim_master 내용 업데이트
        update_sim_master(sim_id=sim_id, free_mac=free_mac, relaxed_time=relaxed_time, status=solver.StatusName(status))




if __name__ == "__main__":
    # ArgumentParser 객체 생성
    parser = argparse.ArgumentParser(description="sim_id 입력")
    parser.add_argument('--sim_id', type=str, help="sim_id")

    # 명령줄 인자 파싱
    args = parser.parse_args()


    main(args.sim_id)
