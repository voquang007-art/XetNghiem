# -*- coding: utf-8 -*-
"""
app/main.py
Mục tiêu của bản tổng thể cho HVGL_WorkXetnghiem:
- Nạp đầy đủ khung router/template/static tối thiểu tương đương HVGL_Workspace
- Giữ các router hiện có của Xét nghiệm
- Bổ sung chat / inbox / units / account_secrets / admin_users để chạy dev tổng thể trước
- Chủ động tạo bảng từ models hiện có + chat models
"""

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException
from starlette.middleware.sessions import SessionMiddleware
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from starlette.responses import RedirectResponse, FileResponse

from .config import settings
from .database import Base, engine, SessionLocal

# Nạp models chính + chat models để Base.metadata.create_all thấy đủ bảng
from . import models  # noqa: F401
from .chat import models as chat_models  # noqa: F401

app = FastAPI(title=getattr(settings, "APP_NAME", "HVGL_WorkXetnghiem"))
app.add_middleware(SessionMiddleware, secret_key=getattr(settings, "SESSION_SECRET", None) or getattr(settings, "SECRET_KEY", "secret"))

static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
app.state.templates = templates

Base.metadata.create_all(bind=engine)


def format_vn_dt(dt: datetime) -> str:
    if not isinstance(dt, datetime):
        return ""
    try:
        import datetime as _dt
        vn_tz = _dt.timezone(_dt.timedelta(hours=7))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_dt.timezone.utc)
        tzdt = dt.astimezone(vn_tz)
    except Exception:
        tzdt = dt
    return f"{tzdt:%d-%m-%Y}-{tzdt:%H}-{tzdt:%M}"


templates.env.filters["format_vn_dt"] = format_vn_dt

from sqlalchemy.orm import Session
from sqlalchemy import or_
from .models import Files, Tasks, Users, Units
try:
    from .models import TaskReports  # type: ignore
    HAS_TASK_REPORTS = True
except Exception:
    TaskReports = None  # type: ignore
    HAS_TASK_REPORTS = False


def list_task_files(task_id: str):
    db: Session = SessionLocal()
    try:
        like_task = f"/TASK/{task_id}/"
        like_reports = f"/TASK/{task_id}/REPORTS/"
        q = (
            db.query(Files)
            .filter(or_(Files.path.like(f"%{like_task}%"), Files.path.like(f"%{like_reports}%")))
            .order_by(Files.uploaded_at.desc() if hasattr(Files, "uploaded_at") else Files.path.desc())
        )
        return q.all()
    finally:
        db.close()


def list_task_report_files(task_id: str):
    db: Session = SessionLocal()
    try:
        patterns = [f"/TASK/{task_id}/REPORTS/"]
        if HAS_TASK_REPORTS and TaskReports is not None:
            reps = db.query(TaskReports).filter(getattr(TaskReports, "task_id") == task_id).all()
            for r in reps:
                rid = getattr(r, "id", None)
                if rid:
                    patterns.append(f"/TASK_REPORT/{rid}/")
        conds = [Files.path.like(f"%{p}%") for p in patterns]
        q = db.query(Files).filter(or_(*conds)).order_by(Files.uploaded_at.desc() if hasattr(Files, "uploaded_at") else Files.path.desc())
        return q.all()
    finally:
        db.close()


def list_task_reports(task_id: str):
    if not HAS_TASK_REPORTS or TaskReports is None:
        return []
    db: Session = SessionLocal()
    try:
        q = db.query(TaskReports).filter(getattr(TaskReports, "task_id") == task_id)
        if hasattr(TaskReports, "reported_at"):
            q = q.order_by(getattr(TaskReports, "reported_at").desc())
        return q.all()
    finally:
        db.close()


def user_unit_names(user_id):
    db: Session = SessionLocal()
    try:
        names = []
        try:
            from .models import UserUnitMemberships as _UserUnits
        except Exception:
            _UserUnits = None
        if _UserUnits is not None and hasattr(_UserUnits, "unit_id"):
            q = (
                db.query(Units)
                .join(_UserUnits, Units.id == _UserUnits.unit_id)
                .filter(getattr(_UserUnits, "user_id") == user_id)
                .order_by(Units.id.asc())
            )
            for u in q.all():
                nm = getattr(u, "ten_don_vi", None) or getattr(u, "name", None) or f"Đơn vị #{getattr(u,'id', '')}"
                names.append(nm)
        else:
            u = db.get(Users, user_id)
            unit_id = getattr(u, "unit_id", None) if u else None
            if unit_id:
                dv = db.get(Units, unit_id)
                if dv:
                    nm = getattr(dv, "ten_don_vi", None) or getattr(dv, "name", None) or f"Đơn vị #{unit_id}"
                    names.append(nm)
        return names
    finally:
        db.close()


templates.env.globals["list_task_files"] = list_task_files
templates.env.globals["list_task_report_files"] = list_task_report_files
templates.env.globals["list_task_reports"] = list_task_reports
templates.env.globals["user_unit_names"] = user_unit_names

from .routers import auth, account, dashboard, draft_approval, files, inbox, leave_schedule, plans, tasks
from .routers import account_secrets, units, admin_users, chat, chat_api, meetings


def include_router_with_log(rtr, prefix: str, tags: list[str], module_name: str):
    app.include_router(rtr, prefix=prefix, tags=tags)
    try:
        print(f"[main] Đã nạp router: {module_name}")
    except Exception:
        pass


include_router_with_log(auth.router, "/auth", ["auth"], "app.routers.auth")
include_router_with_log(account.router, "", ["account"], "app.routers.account")
include_router_with_log(account_secrets.router, "", ["account_secrets"], "app.routers.account_secrets")

include_router_with_log(units.router, "/units", ["units"], "app.routers.units")
include_router_with_log(files.router, "/files", ["files"], "app.routers.files")
include_router_with_log(draft_approval.router, "/draft-approvals", ["draft_approval"], "app.routers.draft_approval")
include_router_with_log(plans.router, "/plans", ["plans"], "app.routers.plans")

include_router_with_log(leave_schedule.router, "", ["leave_schedule"], "app.routers.leave_schedule")
include_router_with_log(chat.router, "", ["chat"], "app.routers.chat")
include_router_with_log(chat_api.router, "", ["chat_api"], "app.routers.chat_api")
include_router_with_log(meetings.router, "", ["meetings"], "app.routers.meetings")
include_router_with_log(dashboard.router, "", ["dashboard"], "app.routers.dashboard")
include_router_with_log(tasks.router, "", ["tasks"], "app.routers.tasks")
include_router_with_log(inbox.router, "", ["inbox"], "app.routers.inbox")

app.include_router(admin_users.router, prefix="/admin")

for _mod in (account, account_secrets, units, files, plans, leave_schedule, tasks, inbox, dashboard, chat, draft_approval, meetings):
    _tpl = getattr(_mod, "templates", None)
    if _tpl is not None and hasattr(_tpl, "env") and hasattr(_tpl.env, "globals"):
        try:
            _tpl.env.globals["user_unit_names"] = user_unit_names
            _tpl.env.filters["format_vn_dt"] = format_vn_dt
        except Exception:
            pass


@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/dashboard", status_code=307)


@app.get("/login", include_in_schema=False)
def login_redirect_get():
    return RedirectResponse(url="/auth/login", status_code=307)


@app.post("/login", include_in_schema=False)
def login_redirect_post():
    return RedirectResponse(url="/auth/login", status_code=307)


@app.get("/logout", include_in_schema=False)
def logout_redirect_get():
    return RedirectResponse(url="/auth/logout", status_code=307)


@app.post("/logout", include_in_schema=False)
def logout_redirect_post():
    return RedirectResponse(url="/auth/logout", status_code=307)


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    cand = os.path.join(static_dir, "images", "favicon.ico")
    if os.path.exists(cand):
        return FileResponse(cand, media_type="image/x-icon")
    raise HTTPException(status_code=404, detail="Not Found")
