# app/routers/inbox.py
# Điều chỉnh theo cơ cấu HVGL_WorkXetnghiem bước 2:
# - dùng login_required để lấy user/session chuẩn
# - chuẩn hóa helper role theo chuỗi quản lý mới
# - inbox vẫn là nơi xem VIỆC ĐƯỢC GIAO CHO TÔI, nên không mở rộng scope ngoài assignee
# - giữ nguyên route/URL/HTML/CSS/DB schema

from __future__ import annotations
from typing import Any, Dict, Iterable, Optional, List, Set
from datetime import datetime
import os
import uuid
import logging

from fastapi import APIRouter, Request, Depends, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.chat.realtime import manager
from app.security.deps import login_required

logger = logging.getLogger("app.inbox")
router = APIRouter()

# ---------- get_db ----------
def _import_get_db():
    for mod in ("app.database", "app.deps", "database", "deps"):
        try:
            m = __import__(mod, fromlist=["get_db"])
            fn = getattr(m, "get_db", None)
            if fn:
                return fn
        except Exception:
            continue
    raise ModuleNotFoundError("Không tìm thấy get_db")
get_db = _import_get_db()

# ---------- templates ----------
def _import_templates():
    for mod, attr in (("app.main", "templates"), ("main", "templates")):
        try:
            m = __import__(mod, fromlist=[attr])
            t = getattr(m, attr, None)
            if t is not None:
                return t
        except Exception:
            continue
    from fastapi.templating import Jinja2Templates
    try:
        return Jinja2Templates(directory="app/templates")
    except Exception:
        return Jinja2Templates(directory="templates")
templates = _import_templates()


def _import_settings():
    for mod, attr in (("app.config", "settings"), ("config", "settings")):
        try:
            m = __import__(mod, fromlist=[attr])
            s = getattr(m, attr, None)
            if s is not None:
                return s
        except Exception:
            continue

    class _FallbackSettings:
        APP_NAME = "QLCV_App"
        COMPANY_NAME = ""

    return _FallbackSettings()


settings = _import_settings()

# ---------- models ----------
try:
    import app.models as models
except Exception:
    models = None


def _get_cls(cands: Iterable[str]):
    if not models:
        return None
    for nm in cands:
        if hasattr(models, nm):
            return getattr(models, nm)
    return None


Users = _get_cls(["Users", "User", "Account"])
Tasks = _get_cls(["Tasks", "Task", "WorkItem", "Job"])
Reports = _get_cls(["TaskReports", "TaskReport", "Reports", "Report"])
Files = _get_cls(["Files", "File", "TaskFiles", "Attachments", "Attachment"])
Roles = _get_cls(["Roles", "Role"])
UserRoles = _get_cls(["UserRoles", "UserRole"])


# ---------- commons ----------
def _me_id(req: Request) -> Optional[str]:
    sess = getattr(req, "session", {}) or {}
    return sess.get("user_id") or (sess.get("user") or {}).get("id")


def _safe_hasattr(cls, name: str) -> bool:
    try:
        return hasattr(cls, name)
    except Exception:
        return False


def _set_if_exist(obj: Any, name: str, value: Any):
    if _safe_hasattr(obj.__class__, name):
        try:
            setattr(obj, name, value)
            return True
        except Exception:
            return False
    return False


def _task_by_id(db: Session, task_id: Any):
    if not Tasks:
        return None
    try:
        return db.query(Tasks).filter(getattr(Tasks, "id") == task_id).first()
    except Exception:
        return None


def _is_creator(task: Any, user_id: Any) -> bool:
    for fld in ("created_by", "creator_user_id", "owner_user_id"):
        v = getattr(task, fld, None)
        if v is not None and str(v) == str(user_id):
            return True
    return False


def _is_assignee(task: Any, user_id: Any) -> bool:
    for fld in ("assignee_id", "assigned_user_id", "assigned_to_user_id", "receiver_user_id"):
        v = getattr(task, fld, None)
        if v is not None and str(v) == str(user_id):
            return True
    return False


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    if not (Roles and UserRoles):
        return set()
    rows = (
        db.query(getattr(Roles, "code"))
        .join(UserRoles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
        .filter(getattr(UserRoles, "user_id") == user_id)
        .all()
    )
    out: Set[str] = set()
    for (c,) in rows:
        out.add(str(getattr(c, "value", c)).upper())
    return out
    

def _is_admin(codes: Set[str]) -> bool:
    return "ROLE_ADMIN" in codes

def _is_employee(codes: Set[str]) -> bool:
    return "ROLE_NHAN_VIEN" in codes


def _is_group_lead(codes: Set[str]) -> bool:
    return bool({"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"} & codes)


def _is_functional_manager(codes: Set[str]) -> bool:
    return bool({
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
    } & codes)


def _is_ktv_truong(codes: Set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG" in codes


def _is_khoa_manager(codes: Set[str]) -> bool:
    return bool({"ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"} & codes)


def _is_bgd(codes: Set[str]) -> bool:
    return "ROLE_BGD" in codes


def _is_board(codes: Set[str]) -> bool:
    return bool({"ROLE_LANH_DAO", "ROLE_HOI_DONG_THANH_VIEN"} & codes)

def _can_access_inbox(codes: Set[str]) -> bool:
    if _is_admin(codes):
        return False
    if _is_board(codes):
        return False
    return True


# ===== Helpers để chuẩn hóa trạng thái (PHỤC VỤ D-1/QUÁ HẠN) =====
def _status_str(x):
    if x is None:
        return ""
    s = getattr(x, "name", None)
    if s:
        return str(s).upper()
    return str(x).upper()


def _is_closed_status(x):
    key = _status_str(x)
    return key in {"DONE", "CLOSED", "CANCELLED", "REJECTED"}


async def _notify_work_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    clean_ids: List[str] = []
    for raw_user_id in user_ids:
        uid = str(raw_user_id or "").strip()
        if uid and uid not in clean_ids:
            clean_ids.append(uid)

    if not clean_ids:
        return

    try:
        await manager.notify_users_json(clean_ids, payload)
    except Exception as ex:
        logger.exception("[/inbox] Notify realtime lỗi: %s", ex)


@router.get("/inbox", response_class=HTMLResponse)
def inbox_view(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    me = getattr(user, "id", None) or _me_id(request)
    if me is None:
        return RedirectResponse(url="/auth/login", status_code=307)

    role_codes = _load_role_codes_for_user(db, str(me))
    if not _can_access_inbox(role_codes):
        return RedirectResponse(url="/tasks", status_code=302)
    rows: List[Any] = []
    try:
        if Tasks:
            q = db.query(Tasks)
            ass_col = None
            for f in ("assignee_id", "assigned_user_id", "assigned_to_user_id", "receiver_user_id"):
                if _safe_hasattr(Tasks, f):
                    ass_col = getattr(Tasks, f)
                    break
            if ass_col is not None:
                q = q.filter(ass_col == me)
            for f in ("closed_at", "archived_at", "deleted_at"):
                if _safe_hasattr(Tasks, f):
                    q = q.filter(getattr(Tasks, f).is_(None))
            order = getattr(Tasks, "created_at", None) or getattr(Tasks, "id")
            rows = q.order_by(order).all()
    except Exception as ex:
        logger.exception("[/inbox] Query lỗi: %s", ex)
        rows = []

    try:
        today = datetime.utcnow().date()
        for t in rows:
            due = None
            for fld in ("due_date", "deadline", "han_hoan_thanh"):
                if _safe_hasattr(Tasks, fld):
                    try:
                        due = getattr(t, fld)
                        if due is not None:
                            break
                    except Exception:
                        pass
            overdue = False
            due_soon = False
            try:
                if due is not None:
                    if hasattr(due, "date"):
                        d = due.date()
                    else:
                        s = str(due)
                        d = datetime.strptime(s[:10], "%Y-%m-%d").date() if ("-" in s and len(s) >= 10) else None
                    if d and not _is_closed_status(getattr(t, "status", None)):
                        delta = (d - today).days
                        overdue = (delta < 0)
                        due_soon = (delta == 1)
            except Exception:
                pass
            try:
                setattr(t, "overdue", overdue)
                setattr(t, "due_soon", due_soon)
            except Exception:
                pass
    except Exception:
        pass

    try:
        ids = [getattr(t, "id", None) for t in rows if getattr(t, "id", None) is not None]
        latest: Dict[Any, Any] = {}
        if Reports and ids:
            try:
                reps = db.query(Reports).filter(getattr(Reports, "task_id").in_(ids)).all()

                def _ts(x):
                    return getattr(x, "reported_at", None) or getattr(x, "created_at", None) or getattr(x, "id", 0)

                for r in reps:
                    tid = getattr(r, "task_id", None)
                    if tid is None:
                        continue
                    if (tid not in latest) or (_ts(r) > _ts(latest[tid])):
                        latest[tid] = r
            except Exception:
                latest = {}

        files_by_task: Dict[Any, List[Dict[str, Any]]] = {tid: [] for tid in ids}
        if Files and ids:
            try:
                qf = db.query(Files)
                col = None
                for c in ("task_id", "related_task_id", "file_task_id"):
                    if _safe_hasattr(Files, c):
                        col = getattr(Files, c)
                        break
                if col is not None:
                    for f in qf.filter(col.in_(ids)).all():
                        p = getattr(f, "path", None) or getattr(f, "storage_path", None)
                        nm = getattr(f, "original_name", None) or getattr(f, "file_name", None) or getattr(f, "name", None)
                        tid2 = getattr(f, "task_id", None) or getattr(f, "related_task_id", None) or getattr(f, "file_task_id", None)
                        if p and (tid2 in files_by_task):
                            files_by_task[tid2].append({"path": p, "name": nm or "tệp"})
            except Exception:
                pass

        for tid, r in latest.items():
            try:
                fp = getattr(r, "file_path", None) or getattr(r, "path", None) or getattr(r, "storage_path", None)
                nm = getattr(r, "original_name", None) or getattr(r, "file_name", None) or getattr(r, "name", None)
                if fp:
                    files_by_task.setdefault(tid, []).append({"path": fp, "name": nm or "tệp"})
            except Exception:
                pass

        for t in rows:
            tid = getattr(t, "id", None)
            r = latest.get(tid)
            note = None
            if r is not None:
                note = getattr(r, "note", None) or getattr(r, "noi_dung", None) or getattr(r, "message", None) or getattr(r, "content", None)
            try:
                setattr(t, "_latest_report_note", note)
                setattr(t, "_files", files_by_task.get(tid, []))
            except Exception:
                pass
    except Exception:
        pass

    ctx = {
        "request": request,
        "tasks": rows,
        "items": rows,
        "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
        "company_name": getattr(settings, "COMPANY_NAME", ""),
        "role_codes": sorted(list(role_codes)),
        "is_admin": _is_admin(role_codes),
        "is_board": _is_board(role_codes),
        "is_bgd": _is_bgd(role_codes),
        "is_khoa_manager": _is_khoa_manager(role_codes),
        "is_ktv_truong": _is_ktv_truong(role_codes),
        "is_functional_manager": _is_functional_manager(role_codes),
        "is_group_lead": _is_group_lead(role_codes),
        "is_employee": _is_employee(role_codes),
    }
    return templates.TemplateResponse("inbox.html", ctx)


@router.post("/inbox/{task_id}/report")
async def report_task(
    request: Request,
    task_id: str,
    note: Optional[str] = Form(None),
    as_feedback: Optional[str] = Form(None),
    file: Optional[UploadFile] = File(None),
    files: Optional[List[UploadFile]] = File(None),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    me = getattr(user, "id", None) or _me_id(request)
    if me is None:
        return RedirectResponse(url="/auth/login", status_code=307)
    role_codes = _load_role_codes_for_user(db, str(me))
    if not _can_access_inbox(role_codes):
        return RedirectResponse(url="/tasks", status_code=302)
    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/inbox", status_code=302)
    if not (_is_assignee(t, me) or _is_creator(t, me)):
        return RedirectResponse(url="/inbox", status_code=302)

    is_feedback = False
    try:
        if as_feedback is not None:
            is_feedback = str(as_feedback).strip().lower() in ("1", "true", "yes", "y", "on")
    except Exception:
        is_feedback = False

    rep = None
    try:
        if Reports:
            rep = Reports()
            _set_if_exist(rep, "task_id", getattr(t, "id", None))
            _set_if_exist(rep, "reported_by", me)
            _set_if_exist(rep, "note", note)
            _set_if_exist(rep, "reported_at", datetime.utcnow())
            _set_if_exist(rep, "created_at", datetime.utcnow())
            if is_feedback:
                _set_if_exist(rep, "status_snapshot", "FEEDBACK")
                _set_if_exist(rep, "type", "FEEDBACK")
                _set_if_exist(rep, "is_feedback", True)
            db.add(rep)
            db.flush()
    except Exception:
        rep = None

    uploads: List[UploadFile] = []
    try:
        if files:
            uploads.extend([u for u in (files or []) if u and getattr(u, "filename", "")])
        if (not uploads) and file and getattr(file, "filename", ""):
            uploads.append(file)
    except Exception:
        pass

    try:
        base_root = os.getenv("UPLOAD_DIR") or os.path.join("instance", "uploads")
        base_dir = os.path.join(base_root, "TASK", str(getattr(t, "id", task_id)), "REPORTS")
        os.makedirs(base_dir, exist_ok=True)

        for up in uploads:
            orig_name = up.filename
            ext = os.path.splitext(orig_name)[1]
            safe_name = f"{uuid.uuid4().hex}{ext or ''}"
            full_path = os.path.join(base_dir, safe_name).replace("\\", "/")

            with open(full_path, "wb") as f:
                f.write(await up.read())

            saved_path = full_path.replace("\\", "/")

            if rep is not None:
                _set_if_exist(rep, "file_path", saved_path)
                _set_if_exist(rep, "original_name", orig_name)

            if Files:
                rec = Files()
                _set_if_exist(rec, "path", saved_path) or _set_if_exist(rec, "storage_path", saved_path)
                _set_if_exist(rec, "original_name", orig_name) or _set_if_exist(rec, "file_name", orig_name)
                _set_if_exist(rec, "created_at", datetime.utcnow())
                _set_if_exist(rec, "created_by", me) or _set_if_exist(rec, "uploader_id", me)
                _set_if_exist(rec, "note", note)
                db.add(rec)

        db.commit()

        try:
            creator_user_id = getattr(t, "created_by", None) or getattr(t, "creator_user_id", None) or getattr(t, "owner_user_id", None)
            assignee_user_id = getattr(t, "assigned_to_user_id", None) or getattr(t, "assigned_user_id", None) or getattr(t, "assignee_id", None) or getattr(t, "receiver_user_id", None)
            event_type = "task_feedback_sent" if is_feedback else "task_reported"
            payload = {
                "module": "work",
                "type": event_type,
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(creator_user_id or ""),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_work_users([str(creator_user_id or ""), str(assignee_user_id or ""), str(me or "")], payload)
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau báo cáo lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] Lỗi lưu báo cáo/tệp: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

    dest = "/inbox"
    try:
        if is_feedback and _is_creator(t, me):
            dest = "/tasks"
    except Exception:
        pass
    return RedirectResponse(url=dest, status_code=302)


@router.post("/inbox/{task_id}/complete")
async def complete_task(request: Request, task_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    me = getattr(user, "id", None) or _me_id(request)
    if me is None:
        return RedirectResponse(url="/auth/login", status_code=307)
    role_codes = _load_role_codes_for_user(db, str(me))
    if not _can_access_inbox(role_codes):
        return RedirectResponse(url="/tasks", status_code=302)
    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/inbox", status_code=302)
    if not _is_assignee(t, me):
        return RedirectResponse(url="/inbox", status_code=302)

    try:
        now_ts = datetime.utcnow()
        _set_if_exist(t, "completed_at", now_ts) or _set_if_exist(t, "finished_at", now_ts)
        _set_if_exist(t, "status", "DONE")
        if _safe_hasattr(Tasks, "updated_at"):
            _set_if_exist(t, "updated_at", now_ts)
        db.add(t)
        db.commit()

        try:
            creator_user_id = getattr(t, "created_by", None) or getattr(t, "creator_user_id", None) or getattr(t, "owner_user_id", None)
            assignee_user_id = getattr(t, "assigned_to_user_id", None) or getattr(t, "assigned_user_id", None) or getattr(t, "assignee_id", None) or getattr(t, "receiver_user_id", None) or me
            payload = {
                "module": "work",
                "type": "task_completed",
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(creator_user_id or ""),
                "timestamp": now_ts.isoformat(),
            }
            await _notify_work_users([str(creator_user_id or ""), str(assignee_user_id or "")], payload)
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau hoàn thành lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] UPDATE lỗi complete: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

    return RedirectResponse(url="/inbox", status_code=302)


@router.post("/inbox/{task_id}/close")
async def close_task(request: Request, task_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    me = getattr(user, "id", None) or _me_id(request)
    if me is None:
        return RedirectResponse(url="/auth/login", status_code=307)

    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/tasks", status_code=302)
    if not _is_creator(t, me):
        return RedirectResponse(url="/tasks", status_code=302)

    try:
        ts = datetime.utcnow()
        if not (_set_if_exist(t, "closed_at", ts) or _set_if_exist(t, "archived_at", ts) or _set_if_exist(t, "deleted_at", ts)):
            _set_if_exist(t, "status", "CLOSED")
        if _safe_hasattr(Tasks, "updated_at"):
            _set_if_exist(t, "updated_at", ts)
        db.add(t)
        db.commit()

        try:
            creator_user_id = getattr(t, "created_by", None) or getattr(t, "creator_user_id", None) or getattr(t, "owner_user_id", None) or me
            assignee_user_id = getattr(t, "assigned_to_user_id", None) or getattr(t, "assigned_user_id", None) or getattr(t, "assignee_id", None) or getattr(t, "receiver_user_id", None)
            payload = {
                "module": "work",
                "type": "task_closed",
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(assignee_user_id or ""),
                "timestamp": ts.isoformat(),
            }
            await _notify_work_users([str(creator_user_id or ""), str(assignee_user_id or "")], payload)
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau kết thúc lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] UPDATE lỗi close: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

    return RedirectResponse(url="/tasks", status_code=302)
