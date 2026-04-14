# app/routers/tasks.py
# Version: 2025-10-24 (+07) – Lucky chỉnh:
# - Bổ sung cờ D-1 (due_soon) và quá hạn (overdue) CÓ XÉT TRẠNG THÁI (loại trừ DONE/CLOSED/CANCELLED/REJECTED)
# - Nạp đầy đủ lịch sử báo cáo (_reports) để phía NGƯỜI GIAO xem toàn bộ ghi chú
# - Giữ nguyên route/URL/DB/schema/import; không ảnh hưởng dashboard/plans

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime, timezone, date
import logging

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_, and_

from app.chat.realtime import manager

try:
    from app.models import ManagementScopes, ScopePermissions, PermissionCode
except Exception:
    ManagementScopes = None
    ScopePermissions = None
    PermissionCode = None

logger = logging.getLogger("app.tasks")
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
    if not models: return None
    for nm in cands:
        if hasattr(models, nm):
            return getattr(models, nm)
    return None

Users   = _get_cls(["Users","User","Account"])
Units   = _get_cls(["Units","Unit"])
Roles   = _get_cls(["Roles","Role"])
UserRoles = _get_cls(["UserRoles","UserRole"])
UserUnitMemberships = _get_cls(["UserUnitMemberships","UserUnits","Memberships"])
Tasks   = _get_cls(["Tasks","Task","WorkItem","Job"])
Files   = _get_cls(["Files","File","TaskFiles","Attachments","Attachment"])
Reports = _get_cls(["TaskReports","TaskReport","Reports","Report"])
UserStatus = getattr(models, "UserStatus", None)

# ---------- helpers chung ----------
def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def _current_user_id(req: Request) -> Optional[int]:
    sess = getattr(req, "session", {}) or {}
    return sess.get("user_id") or (sess.get("user") or {}).get("id")

def _safe_get(o: Any, fields: Iterable[str]):
    for f in fields:
        if hasattr(o, f):
            try:
                v = getattr(o, f)
                if v is not None:
                    return v
            except Exception:
                pass
    return None

def _assignee_id_of(t) -> Any:
    return _safe_get(t, ("assignee_id","assigned_user_id","assigned_to_user_id","receiver_user_id"))

def _due_of(t) -> Optional[datetime]:
    return _safe_get(t, ("due_date","deadline","han_hoan_thanh"))

# ===== Trạng thái (phục vụ D-1/Quá hạn) =====
def _status_str(x):
    if x is None:
        return ""
    s = getattr(x, "name", None)
    if s:
        return str(s).upper()
    return str(x).upper()

def _is_closed_status(x) -> bool:
    key = _status_str(x)
    return key in {"DONE", "CLOSED", "CANCELLED", "REJECTED"}

async def _notify_work_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    """
    Phát sự kiện realtime dùng chung qua notify socket hiện có của chat.
    Dùng await trực tiếp để tránh lỗi NoEventLoopError.
    """
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
        logger.exception("[tasks] Notify realtime lỗi: %s", ex)
        
# ---------- normalize & enrich ----------
def _normalize_status_data(db: Session):
    """Chuẩn hóa status bị lạc (COMPLETED/FINISHED -> DONE)."""
    if not Tasks or not hasattr(Tasks, "__tablename__") or not hasattr(Tasks, "status"):
        return
    try:
        table = getattr(Tasks, "__tablename__")
        db.execute(text(f"UPDATE {table} SET status='DONE' WHERE status IN ('COMPLETED','FINISHED')"))
        db.commit()
    except Exception:
        try: db.rollback()
        except Exception: pass

def _enrich_reports_and_files(db: Session, tasks: List[Any]) -> Dict[Any, Dict[str, Any]]:
    out: Dict[Any, Dict[str, Any]] = {}
    if not tasks or (not Reports and not Files):
        return out
    ids = [getattr(t,"id",None) for t in tasks if getattr(t,"id",None) is not None]
    if not ids:
        return out

    latest: Dict[Any, Any] = {}
    if Reports:
        try:
            rows = db.query(Reports).filter(getattr(Reports,"task_id").in_(ids)).all()
            def ts(r): return _safe_get(r, ("reported_at","created_at")) or getattr(r,"id",0)
            for r in rows:
                tid = getattr(r, "task_id", None)
                if tid is None: 
                    continue
                if (tid not in latest) or (ts(r) > ts(latest[tid])):
                    latest[tid] = r
        except Exception:
            latest = {}

    files_by_task: Dict[Any, List[Dict[str, Any]]] = {tid: [] for tid in ids}
    if Files:
        try:
            q = db.query(Files)
            col = None
            for c in ("task_id","related_task_id","file_task_id"):
                if hasattr(Files, c):
                    col = getattr(Files, c); break
            if col is not None:
                for f in q.filter(col.in_(ids)).all():
                    p = _safe_get(f, ("path","storage_path"))
                    nm = _safe_get(f, ("original_name","file_name","name"))
                    tid2 = getattr(f, "task_id", None) or _safe_get(f, ("related_task_id","file_task_id"))
                    if p and (tid2 in files_by_task):
                        files_by_task[tid2].append({"path": p, "name": nm or "tệp"})
        except Exception:
            pass

    for tid, r in latest.items():
        try:
            fp = _safe_get(r, ("file_path","path","storage_path"))
            nm = _safe_get(r, ("original_name","file_name","name"))
            if fp:
                files_by_task.setdefault(tid, []).append({"path": fp, "name": nm or "tệp"})
        except Exception:
            pass

    for tid in ids:
        note = _safe_get(latest.get(tid) or {}, ("note","noi_dung","message","content"))
        out[tid] = {"latest_note": note, "files": files_by_task.get(tid, [])}
    return out

# ---------- quyền/role & phạm vi đơn vị ----------
def _current_user_from_request(request: Request, db: Session) -> Optional[Any]:
    uid = request.session.get("user_id") if hasattr(request, "session") else None
    if uid and Users:
        try:
            u = db.get(Users, uid)
            if u: return u
        except Exception:
            pass
    uname = request.session.get("username") if hasattr(request, "session") else None
    if uname and Users:
        try:
            u = db.query(Users).filter(getattr(Users, "username")==uname).first()
            if u: return u
        except Exception:
            pass
    return None

def _role_codes_for_user(db: Session, user: Optional[Any]) -> Set[str]:
    if not user or not (Roles and UserRoles):
        return set()
    codes: Set[str] = set()
    try:
        rows = (
            db.query(getattr(Roles,"code"))
              .join(UserRoles, getattr(UserRoles,"role_id")==getattr(Roles,"id"))
              .filter(getattr(UserRoles,"user_id")==getattr(user,"id"))
              .all()
        )
        for (c,) in rows:
            codes.add(str(getattr(c,"value", c)).upper())
    except Exception:
        pass
    return codes

def _has_any_role(codes: Set[str], wanted: Iterable[str]) -> bool:
    return bool(set(wanted) & set(codes or set()))

def _is_admin(codes: Set[str]) -> bool:
    return "ROLE_ADMIN" in (codes or set())

def _is_board(codes: Set[str]) -> bool:
    return _has_any_role(codes, {"ROLE_LANH_DAO", "ROLE_HOI_DONG_THANH_VIEN"})

def _is_admin_or_ld(codes: Set[str]) -> bool:
    return _is_admin(codes) or _is_board(codes)

def _is_bgd(codes: Set[str]) -> bool:
    return "ROLE_BGD" in (codes or set())

def _is_truong_khoa(codes: Set[str]) -> bool:
    return "ROLE_TRUONG_KHOA" in (codes or set())

def _is_pho_khoa(codes: Set[str]) -> bool:
    return "ROLE_PHO_TRUONG_KHOA" in (codes or set())

def _is_bgd_or_lab_lead(codes: Set[str]) -> bool:
    return _is_bgd(codes) or _is_truong_khoa(codes) or _is_pho_khoa(codes)

def _is_ktv_truong(codes: Set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG" in (codes or set())

def _is_functional_manager(codes: Set[str]) -> bool:
    return _has_any_role(codes, {
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
    })

def _is_operations_manager(codes: Set[str]) -> bool:
    return _has_any_role(codes, {
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
        "ROLE_QL_CONG_VIEC",
    })

def _is_truong_nhom(codes: Set[str]) -> bool:
    return _has_any_role(codes, {"ROLE_TRUONG_NHOM", "ROLE_TO_TRUONG"})

def _is_pho_nhom(codes: Set[str]) -> bool:
    return _has_any_role(codes, {"ROLE_PHO_NHOM", "ROLE_PHO_TO"})

def _is_group_lead(codes: Set[str]) -> bool:
    return _is_truong_nhom(codes) or _is_pho_nhom(codes)

def _is_manager_role(codes: Set[str]) -> bool:
    return (
        _is_bgd(codes)
        or _is_truong_khoa(codes)
        or _is_pho_khoa(codes)
        or _is_ktv_truong(codes)
        or _is_functional_manager(codes)
        or _is_operations_manager(codes)
        or _is_group_lead(codes)
    )

def _is_matrix_manager(codes: Set[str]) -> bool:
    return _is_bgd_or_lab_lead(codes) or _is_ktv_truong(codes) or _is_functional_manager(codes) or _is_operations_manager(codes)

def _managed_scope_unit_ids(db: Session, user: Optional[Any]) -> Set[str]:
    if not user or not ManagementScopes:
        return set()
    now = datetime.utcnow()
    out: Set[str] = set()
    try:
        q = db.query(ManagementScopes).filter(ManagementScopes.manager_user_id == getattr(user, "id", None))
        if hasattr(ManagementScopes, "is_active"):
            q = q.filter(ManagementScopes.is_active == True)
        rows = q.all()
        for row in rows:
            if getattr(row, "effective_from", None) and row.effective_from > now:
                continue
            if getattr(row, "effective_to", None) and row.effective_to < now:
                continue
            tu = getattr(row, "target_unit_id", None)
            if tu:
                out.add(str(tu))
        return out
    except Exception:
        return set()

def _descendant_unit_ids(db: Session, base_ids: Iterable[str]) -> Set[str]:
    ids = {str(x) for x in (base_ids or []) if x}
    if not ids or not Units:
        return ids
    try:
        res = set(ids)
        pending = list(ids)
        while pending:
            rows = db.query(getattr(Units, "id")).filter(getattr(Units, "parent_id").in_(pending)).all()
            child_ids = {str(r[0]) for r in rows if r and r[0] and str(r[0]) not in res}
            if not child_ids:
                break
            res.update(child_ids)
            pending = list(child_ids)
        return res
    except Exception:
        return ids

def _group_lead_unit_ids(db: Session, user: Optional[Any]) -> Set[str]:
    if not user or not (Units and UserUnitMemberships):
        return set()
    try:
        rows = (
            db.query(getattr(UserUnitMemberships, "unit_id"), getattr(Units, "cap_do"))
              .join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
              .filter(getattr(UserUnitMemberships, "user_id") == getattr(user, "id"))
              .all()
        )
    except Exception:
        rows = []

    team_ids = {str(unit_id) for unit_id, cap_do in rows if unit_id and cap_do == 3}
    if team_ids:
        return team_ids
    return {str(unit_id) for unit_id, _cap_do in rows if unit_id}

def _unit_scope_for_user(db: Session, user: Optional[Any], codes: Set[str]) -> Tuple[Set[str], str]:
    if not user or not (Units and UserUnitMemberships):
        return set(), "Chưa đăng nhập"

    base_ids: Set[str] = set()
    try:
        rows = db.query(getattr(UserUnitMemberships, "unit_id")).filter(getattr(UserUnitMemberships, "user_id") == getattr(user, "id")).all()
        base_ids = {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        base_ids = set()

    matrix_ids = _managed_scope_unit_ids(db, user)

    if _is_admin_or_ld(codes):
        try:
            all_ids = {str(r[0]) for r in db.query(getattr(Units, "id")).all() if r and r[0]}
            return all_ids, "Admin / Lãnh đạo hệ thống (toàn bộ đơn vị)"
        except Exception:
            return base_ids | matrix_ids, "Admin / Lãnh đạo hệ thống"

    if _is_bgd_or_lab_lead(codes) or _is_ktv_truong(codes):
        try:
            all_ids = {str(r[0]) for r in db.query(getattr(Units, "id")).all() if r and r[0]}
            return all_ids, "BGĐ / Khoa xét nghiệm" if _is_bgd_or_lab_lead(codes) else "Kỹ thuật viên trưởng (toàn bộ khoa xét nghiệm)"
        except Exception:
            return base_ids | matrix_ids, "BGĐ / Khoa xét nghiệm"

    if _is_functional_manager(codes) or _is_operations_manager(codes):
        scoped = set(base_ids) | set(matrix_ids)
        if scoped:
            scoped = _descendant_unit_ids(db, scoped)
        return scoped, "Quản lý ma trận (đơn vị phụ trách và các nhóm được phân công)"

    if _is_group_lead(codes):
        scoped = _group_lead_unit_ids(db, user)
        if scoped:
            return scoped, "Trưởng nhóm / Phó nhóm (chỉ nhóm, tổ của bạn)"
        return base_ids, "Trưởng nhóm / Phó nhóm (chỉ nhóm, tổ của bạn)"

    return base_ids, "Nhân viên (chỉ phạm vi trực thuộc)"

def _query_users_in_units_by_roles(
    db: Session,
    unit_ids: Iterable[str],
    role_codes: Iterable[str],
    expected_unit_level: Optional[int] = None,
) -> List[Any]:
    if not (Users and Roles and UserRoles and UserUnitMemberships and Units):
        return []
    wanted = [str(c).upper() for c in role_codes]
    try:
        q = (
            db.query(Users)
              .join(UserUnitMemberships, getattr(UserUnitMemberships,"user_id")==getattr(Users,"id"))
              .join(UserRoles, getattr(UserRoles,"user_id")==getattr(Users,"id"))
              .join(Roles, getattr(Roles,"id")==getattr(UserRoles,"role_id"))
              .join(Units, getattr(Units,"id")==getattr(UserUnitMemberships,"unit_id"))
              .filter(getattr(UserUnitMemberships,"unit_id").in_(list(unit_ids)))
              .filter(func.upper(func.coalesce(getattr(Roles,"code"), "")).in_(wanted))
        )
        if UserStatus and hasattr(Users, "status"):
            q = q.filter(getattr(Users,"status")==UserStatus.ACTIVE)
        if expected_unit_level is not None and hasattr(Units, "cap_do"):
            q = q.filter(getattr(Units,"cap_do")==expected_unit_level)

        order_col = (
            (getattr(Users, "full_name", None) or
             getattr(Users, "name", None) or
             getattr(Users, "username", None) or
             getattr(Users, "id"))
        )
        return q.distinct().order_by(order_col).all()
    except Exception:
        return []

# ====== BỔ SUNG: NV trực thuộc PHÒNG (cap_do=2) cho QL PHÒNG ======
def _query_nv_truc_thuoc_phong(db: Session, unit_ids: Iterable[str]) -> List[Any]:
    """
    Lấy nhân viên ROLE_NHAN_VIEN có membership gắn trực tiếp vào đơn vị cấp PHÒNG (cap_do = 2)
    trong phạm vi unit_ids (bao gồm cả khi unit_ids chứa tổ — ta lọc theo cap_do=2).
    """
    if not (Users and Roles and UserRoles and UserUnitMemberships and Units):
        return []
    try:
        wanted = ["ROLE_NHAN_VIEN"]
        q = (
            db.query(Users)
              .join(UserUnitMemberships, getattr(UserUnitMemberships,"user_id")==getattr(Users,"id"))
              .join(Units, getattr(Units,"id")==getattr(UserUnitMemberships,"unit_id"))
              .join(UserRoles, getattr(UserRoles,"user_id")==getattr(Users,"id"))
              .join(Roles, getattr(Roles,"id")==getattr(UserRoles,"role_id"))
              .filter(func.upper(func.coalesce(getattr(Roles,"code"), "" )).in_(wanted))
        )
        if hasattr(Units, "cap_do"):
            q = q.filter(getattr(Units,"cap_do")==2)
        q = q.filter(getattr(Units,"id").in_(list(unit_ids)))

        if UserStatus and hasattr(Users, "status"):
            q = q.filter(getattr(Users,"status")==UserStatus.ACTIVE)

        order_col = (
            (getattr(Users, "full_name", None) or
             getattr(Users, "name", None) or
             getattr(Users, "username", None) or
             getattr(Users, "id"))
        )
        return q.distinct().order_by(order_col).all()
    except Exception:
        return []

def _merge_distinct_users(*groups: List[Any], exclude_user_id: Optional[Any] = None) -> List[Any]:
    out: List[Any] = []
    seen: Set[str] = set()
    for group in groups:
        for u in (group or []):
            uid = getattr(u, "id", None)
            if uid is None:
                continue
            uid_s = str(uid)
            if exclude_user_id is not None and uid_s == str(exclude_user_id):
                continue
            if uid_s in seen:
                continue
            seen.add(uid_s)
            out.append(u)
    return out


def _recipient_ids_from_ctx(ctx: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for key in (
        "recipients_khoa",
        "recipients_functional",
        "recipients_execution",
        "recipients_staff",
    ):
        for u in (ctx.get(key) or []):
            uid = getattr(u, "id", None)
            if uid is not None:
                out.add(str(uid))
    return out


def _compute_assignment_context(
    db: Session,
    user: Optional[Any],
    unit_scope: Set[str],
    scope_label: str,
    codes: Set[str],
) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {
        "can_assign": False,
        "scope_units": list(unit_scope),
        "scope_label": scope_label,
        "recipients_khoa": [],
        "recipients_functional": [],
        "recipients_execution": [],
        "recipients_staff": [],
    }
    if not user:
        return ctx

    my_id = getattr(user, "id", None)

    if _is_admin_or_ld(codes) or _is_manager_role(codes):
        ctx["can_assign"] = True

    if not ctx["can_assign"]:
        return ctx

    # Admin / HĐTV: giao việc toàn hệ thống
    if _is_admin_or_ld(codes):
        ctx["scope_label"] = "Admin / HĐTV (giao việc toàn hệ thống)"
        ctx["recipients_khoa"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_BGD", "ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_functional"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_KY_THUAT_VIEN_TRUONG", "ROLE_QL_CHAT_LUONG", "ROLE_QL_KY_THUAT", "ROLE_QL_AN_TOAN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_execution"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_VAT_TU", "ROLE_QL_TRANG_THIET_BI", "ROLE_QL_MOI_TRUONG", "ROLE_QL_CNTT", "ROLE_QL_CONG_VIEC", "ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_staff"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_NHAN_VIEN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # BGĐ: Trưởng khoa, Phó khoa
    if _is_bgd(codes):
        ctx["scope_label"] = "BGĐ (chỉ giao cho Trưởng khoa, Phó khoa)"
        ctx["recipients_khoa"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # Trưởng khoa: Phó khoa, KTV trưởng, QL chức năng
    if _is_truong_khoa(codes):
        ctx["scope_label"] = "Trưởng khoa (Phó khoa, KTV trưởng, Quản lý chức năng)"
        ctx["recipients_khoa"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_PHO_TRUONG_KHOA", "ROLE_KY_THUAT_VIEN_TRUONG"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_functional"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_CHAT_LUONG", "ROLE_QL_KY_THUAT", "ROLE_QL_AN_TOAN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # Phó khoa: KTV trưởng, QL chức năng
    if _is_pho_khoa(codes):
        ctx["scope_label"] = "Phó khoa (KTV trưởng, Quản lý chức năng)"
        ctx["recipients_khoa"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_KY_THUAT_VIEN_TRUONG"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_functional"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_CHAT_LUONG", "ROLE_QL_KY_THUAT", "ROLE_QL_AN_TOAN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # KTV trưởng: QL chức năng + QL công việc + nhóm trưởng/nhóm phó
    if _is_ktv_truong(codes):
        ctx["scope_label"] = "Kỹ thuật viên trưởng (Quản lý chức năng, Quản lý công việc, Trưởng/Phó nhóm)"
        ctx["recipients_functional"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_CHAT_LUONG", "ROLE_QL_KY_THUAT", "ROLE_QL_AN_TOAN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_execution"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_VAT_TU", "ROLE_QL_TRANG_THIET_BI", "ROLE_QL_MOI_TRUONG", "ROLE_QL_CNTT", "ROLE_QL_CONG_VIEC", "ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # QL chức năng: QL công việc + nhóm trưởng/nhóm phó
    if _is_functional_manager(codes):
        ctx["scope_label"] = "Quản lý chức năng (Quản lý công việc, Trưởng/Phó nhóm)"
        ctx["recipients_execution"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_QL_VAT_TU", "ROLE_QL_TRANG_THIET_BI", "ROLE_QL_MOI_TRUONG", "ROLE_QL_CNTT", "ROLE_QL_CONG_VIEC", "ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # QL công việc: nhóm trưởng/nhóm phó
    if _is_operations_manager(codes):
        ctx["scope_label"] = "Quản lý công việc (Trưởng/Phó nhóm)"
        ctx["recipients_execution"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # Trưởng nhóm: Phó nhóm + nhân viên nhóm mình
    if _is_truong_nhom(codes):
        ctx["scope_label"] = "Trưởng nhóm (Phó nhóm và Nhân viên của nhóm mình)"
        ctx["recipients_execution"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_PHO_NHOM", "ROLE_PHO_TO"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        ctx["recipients_staff"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_NHAN_VIEN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    # Phó nhóm: nhân viên nhóm mình
    if _is_pho_nhom(codes):
        ctx["scope_label"] = "Phó nhóm (Nhân viên của nhóm mình)"
        ctx["recipients_staff"] = _merge_distinct_users(
            _query_users_in_units_by_roles(
                db,
                unit_scope,
                ["ROLE_NHAN_VIEN"],
                expected_unit_level=None,
            ),
            exclude_user_id=my_id,
        )
        return ctx

    ctx["can_assign"] = False
    return ctx
# ========================= ROUTES =========================
@router.get("/tasks", response_class=HTMLResponse)
def tasks_list(request: Request, db: Session = Depends(get_db)):
    me_id = _current_user_id(request)
    if me_id is None:
        return RedirectResponse(url="/login", status_code=307)

    _normalize_status_data(db)

    me = _current_user_from_request(request, db)
    codes = _role_codes_for_user(db, me)
    unit_scope, scope_label = _unit_scope_for_user(db, me, codes)
    assign_ctx = _compute_assignment_context(db, me, unit_scope, scope_label, codes)
    if not assign_ctx.get("can_assign"):
        return RedirectResponse(url="/inbox", status_code=302)
    rows: List[Any] = []
    try:
        if Tasks:
            q = db.query(Tasks)
            for fld in ("closed_at","archived_at","deleted_at"):
                if hasattr(Tasks, fld):
                    q = q.filter(getattr(Tasks, fld).is_(None))

            creator = None
            for f in ("created_by","creator_user_id","owner_user_id"):
                if hasattr(Tasks, f):
                    creator = getattr(Tasks, f)
                    break

            assignee_col = None
            for f in ("assigned_to_user_id","assigned_user_id","assignee_id","receiver_user_id"):
                if hasattr(Tasks, f):
                    assignee_col = getattr(Tasks, f)
                    break

            visible_user_ids = set()
            if unit_scope and UserUnitMemberships:
                try:
                    visible_user_ids = {str(r[0]) for r in db.query(getattr(UserUnitMemberships, "user_id")).filter(getattr(UserUnitMemberships, "unit_id").in_(list(unit_scope))).all() if r and r[0]}
                except Exception:
                    visible_user_ids = set()

            conds = []
            if creator is not None:
                conds.append(creator == me_id)
                if _is_admin_or_ld(codes) or _is_bgd_or_lab_lead(codes) or _is_ktv_truong(codes) or _is_functional_manager(codes) or _is_operations_manager(codes) or _is_group_lead(codes):
                    if visible_user_ids:
                        conds.append(creator.in_(list(visible_user_ids)))
            if assignee_col is not None:
                conds.append(assignee_col == me_id)
                if (_is_admin_or_ld(codes) or _is_bgd_or_lab_lead(codes) or _is_ktv_truong(codes) or _is_functional_manager(codes) or _is_operations_manager(codes) or _is_group_lead(codes)) and visible_user_ids:
                    conds.append(assignee_col.in_(list(visible_user_ids)))

            if conds:
                q = q.filter(or_(*conds))

            order = getattr(Tasks, "created_at", None) or getattr(Tasks, "id")
            rows = q.order_by(order.desc() if hasattr(order, 'desc') else order).all()
    except Exception as ex:
        logger.exception("[/tasks] Query lỗi: %s", ex)
        rows = []

    # ===== Tên người nhận =====
    assignee_names: Dict[Any,str] = {}
    try:
        if Users:
            ids = list({ _assignee_id_of(t) for t in rows if _assignee_id_of(t) is not None })
            if ids:
                for u in db.query(Users).filter(getattr(Users,"id").in_(ids)).all():
                    name = _safe_get(u, ("full_name","display_name","username","name","email")) or "-"
                    assignee_names[getattr(u,"id",None)] = name
    except Exception:
        pass

    # ===== Ghi chú gần nhất + files (giữ nguyên) =====
    info = _enrich_reports_and_files(db, rows)

    # ===== Lịch sử báo cáo đầy đủ (bổ sung) =====
    reports_map: Dict[Any, List[Dict[str, Any]]] = {}
    try:
        if Reports and rows:
            task_ids = [getattr(t,"id",None) for t in rows if getattr(t,"id",None) is not None]
            if task_ids:
                q = db.query(Reports).filter(getattr(Reports, "task_id").in_(task_ids))
                # sắp theo thời gian tăng dần để đọc mạch lạc
                q = q.order_by(getattr(Reports, "reported_at", getattr(Reports, "created_at", None)).asc())
                for r in q.all():
                    tid = getattr(r, "task_id", None)
                    if tid is None:
                        continue
                    rec = {
                        "id": getattr(r, "id", None),
                        "note": _safe_get(r, ("note","ghi_chu")) or "",
                        "at": _safe_get(r, ("reported_at","created_at")),
                        "user": _safe_get(r, ("user_display_name","user_name","created_by_name")),
                        "files": [],  # có thể gắn nếu có bảng Files liên kết theo report_id; để trống nếu không có
                    }
                    reports_map.setdefault(tid, []).append(rec)
    except Exception:
        reports_map = {}

    # ===== Cờ D-1/Quá hạn theo trạng thái (bổ sung loại trừ) =====
    today = datetime.utcnow().date()
    for t in rows:
        setattr(t, "_assignee_name", assignee_names.get(_assignee_id_of(t)))
        ii = info.get(getattr(t,"id",None), {})
        setattr(t, "_latest_report_note", ii.get("latest_note"))
        setattr(t, "_files", ii.get("files", []))
        # Lịch sử đầy đủ
        setattr(t, "_reports", reports_map.get(getattr(t,"id",None), []))

        due = _due_of(t)
        overdue = False; due_soon = False
        try:
            if due is not None:
                d = due.date() if hasattr(due,"date") else None
                if not d and isinstance(due, str) and "-" in due:
                    d = datetime.strptime(due[:10], "%Y-%m-%d").date()
                if d:
                    # CHỈ cảnh báo khi chưa thuộc nhóm hoàn tất/không cần cảnh báo
                    if not _is_closed_status(_safe_get(t, ("status","trang_thai"))):
                        delta = (d - today).days
                        overdue = (delta < 0)
                        due_soon = (delta == 1)
        except Exception:
            pass
        setattr(t, "_overdue", overdue)
        setattr(t, "_due_soon", due_soon)

    ctx = {
        "request": request,
        "tasks": rows,
        "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
        "company_name": getattr(settings, "COMPANY_NAME", ""),
        **assign_ctx,
    }
    return templates.TemplateResponse("tasks.html", ctx)

@router.post("/tasks/assign")
async def assign_task(
    request: Request,
    title: str = Form(...),
    assignee_id: str = Form(...),
    due_date: Optional[str] = Form(None),
    content: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    """Giữ nguyên hành vi; giao cho người cụ thể; không đổi form/route."""
    me = _current_user_from_request(request, db)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    codes = _role_codes_for_user(db, me)
    unit_scope, scope_label = _unit_scope_for_user(db, me, codes)
    assign_ctx = _compute_assignment_context(db, me, unit_scope, scope_label, codes)

    if not assign_ctx.get("can_assign"):
        return RedirectResponse(url="/inbox", status_code=302)

    allowed_recipient_ids = _recipient_ids_from_ctx(assign_ctx)
    if str(assignee_id or "") not in allowed_recipient_ids:
        logger.warning(
            "[assign] Chặn giao việc sai phạm vi. user_id=%s assignee_id=%s",
            getattr(me, "id", None),
            assignee_id,
        )
        return RedirectResponse(url="/tasks", status_code=302)
    def _first_unit_id_of_user(user_id: Any) -> Optional[Any]:
        if not UserUnitMemberships: return None
        try:
            row = (
                db.query(getattr(UserUnitMemberships,"unit_id"))
                  .filter(getattr(UserUnitMemberships,"user_id")==user_id)
                  .order_by(getattr(UserUnitMemberships,"unit_id").asc())
                  .first()
            )
            return row[0] if row else None
        except Exception:
            return None

    unit_id = None
    if me:
        unit_id = _first_unit_id_of_user(getattr(me,"id",None))
    if unit_id is None:
        unit_id = _first_unit_id_of_user(assignee_id)

    due_dt = None
    if due_date:
        try:
            due_dt = datetime.strptime(due_date, "%Y-%m-%d")
        except Exception:
            due_dt = None

    try:
        TasksCls = Tasks
        if TasksCls is None and models is not None:
            for name in ("Tasks","Task","WorkItem","Job"):
                if hasattr(models, name):
                    TasksCls = getattr(models, name); break
        if TasksCls is None:
            logger.warning("[assign] Không tìm thấy lớp Tasks – bỏ qua ghi DB.")
            return RedirectResponse(url="/tasks", status_code=302)

        t = TasksCls()

        if unit_id is not None and hasattr(t, "unit_id"):
            t.unit_id = unit_id
        if hasattr(t, "title"): t.title = title
        elif hasattr(t, "name"): t.name = title

        if content:
            if hasattr(t, "description"): t.description = content
            elif hasattr(t, "content"):   t.content = content

        if due_dt is not None:
            for fld in ("due_date","deadline"):
                if hasattr(t, fld): setattr(t, fld, due_dt); break

        if me:
            for fld in ("created_by","creator_user_id","owner_user_id"):
                if hasattr(t, fld): setattr(t, fld, getattr(me,"id",None)); break

        for fld in ("assigned_to_user_id","assigned_user_id","assignee_id","receiver_user_id"):
            if hasattr(t, fld): setattr(t, fld, assignee_id); break

        now = datetime.utcnow()
        for fld in ("assigned_at","received_at","created_at","created"):
            if hasattr(t, fld) and getattr(t, fld, None) in (None, ""):
                setattr(t, fld, now)

        db.add(t)
        db.commit()

        try:
            task_id = getattr(t, "id", None)
            creator_id = getattr(me, "id", None) if me else None
            assignee_user_id = (
                getattr(t, "assigned_to_user_id", None)
                or getattr(t, "assigned_user_id", None)
                or getattr(t, "assignee_id", None)
                or getattr(t, "receiver_user_id", None)
                or assignee_id
            )

            payload = {
                "module": "work",
                "type": "task_assigned",
                "task_id": str(task_id or ""),
                "from_user_id": str(creator_id or ""),
                "to_user_id": str(assignee_user_id or ""),
                "title": title or "",
                "timestamp": datetime.utcnow().isoformat(),
            }

            await _notify_work_users(
                [str(assignee_user_id or ""), str(creator_id or "")],
                payload,
            )
        except Exception as ex:
            logger.exception("[assign] Notify realtime lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[assign] Lỗi ghi DB: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

    return RedirectResponse(url="/tasks", status_code=302)
