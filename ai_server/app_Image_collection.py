from apscheduler.schedulers.background import BackgroundScheduler
from blueprints.cctv_alarm import cctv_alarm
from blueprints.cctv_CRUD import cctv_crud
from blueprints.cctv_process import cctv_process
from blueprints.cctv_remote import cctv_remote
from blueprints.log import cctv_log
from blueprints.master_event import ce
from blueprints.master_model import cctv_model
from blueprints.master_monitoring import cctv_pro_detail
from blueprints.master_roi import cctv_roi
from blueprints.monitoring_profile import cctv_profile
from blueprints.server_CRUD import cctv_server
from blueprints.Simulation import cctv_sim
from blueprints.dt_monitoring import cctv_dt_monitor
from blueprints.Legacy import legacy
from blueprints.user import cctv_user
from flask import Flask
# from grid.grid_CRUD import grid_crud
# from grid.grid_CRUD import clean_up_non_updated_states
from blueprints.dt_CRUD import dt_crud
from blueprints.dt_CRUD_remote import dt_crud_remote
from blueprints.safety_manager_CRUD import manager_crud
from blueprints.DT_model import dt_model
from blueprints.work_space import work_space

# from flask_cors import CORS
from datetime import datetime, timedelta

_bootstrap_try = 0
_bootstrapped = False
app = Flask(__name__)
# CORS(app)  # 모든 도메인 허용

# 블루프린트 등록
app.register_blueprint(cctv_alarm)
app.register_blueprint(cctv_process)
app.register_blueprint(cctv_server)
app.register_blueprint(cctv_crud)
app.register_blueprint(cctv_user)
app.register_blueprint(cctv_remote)
app.register_blueprint(cctv_profile)
app.register_blueprint(cctv_pro_detail)
app.register_blueprint(cctv_model)
app.register_blueprint(cctv_roi)
app.register_blueprint(cctv_log)
app.register_blueprint(ce)
app.register_blueprint(dt_crud)
app.register_blueprint(dt_crud_remote)
app.register_blueprint(dt_model)
app.register_blueprint(manager_crud)
app.register_blueprint(cctv_sim)
app.register_blueprint(legacy)
app.register_blueprint(cctv_dt_monitor)
app.register_blueprint(work_space)

# 스케줄러 초기화 및 작업 등록
scheduler = BackgroundScheduler()

#scheduler.add_job(clean_up_non_updated_states, 'interval', days=7)  # 7일마다 실행
def bootstrap_job():
    global _bootstrap_try
    _bootstrap_try += 1
    try:
        from blueprints.cctv_remote import bootstrap_run_enabled_cctv
        bootstrap_run_enabled_cctv()
        print("[BOOTSTRAP] success", flush=True)
    except Exception as e:
        print(f"[BOOTSTRAP] fail({_bootstrap_try}): {e}", flush=True)
        if _bootstrap_try < 20:
            scheduler.add_job(
                bootstrap_job,
                'date',
                run_date=datetime.now() + timedelta(seconds=5)
            )

scheduler.add_job(bootstrap_job, 'date', run_date=datetime.now() + timedelta(seconds=2))

# scheduler.add_job(clean_up_non_updated_states, 'interval', days=7)
scheduler.start()

if __name__ == '__main__':
    try:
        app.run(host="0.0.0.0", debug=False, port=7777)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


