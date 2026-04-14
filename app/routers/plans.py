from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_
from typing import List, Optional, Dict, Set, Iterable, Any
import os
import logging
import asyncio
from datetime import datetime

# GIỮ NGUYÊN import & cấu trúc
from ..security.deps import get_db, login_required, user_has_any_role
from ..security.secret_lock import require_secret_lock
from ..security.policy import ActionCode
from ..security.scope import accessible_units, accessible_unit_ids, is_all_units_access
from ..models import (
    Users, Units, UserUnitMemberships,
    Plans, PlanItems, PlanStatus,
    Roles, RoleCode, UnitStatus,
    VisibilityGrants,
    ManagementScopes
)
from ..config import settings
from app.chat.realtime import manager
router = APIRouter(tags=["plans"])
logger = logging.getLogger("app.plans")
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))
templates.env.globals["now"] = lambda: datetime.utcnow()

# TẮT secret lock cho Plans (không đổi cấu trúc)
_ENABLE_SECRET_LOCK_PLANS = False
def _skip_secret_lock():
    return None

_PLAN_KIND_LABELS = {
    "NHANVIEN": "Kế hoạch cá nhân",
    "NHOM": "Kế hoạch nhóm",
    "KHOA": "Kế hoạch khoa",
    "CHUCNANG_CHAT_LUONG": "Kế hoạch chức năng - Chất lượng",
    "CHUCNANG_KY_THUAT": "Kế hoạch chức năng - Kỹ thuật",
    "CHUCNANG_AN_TOAN": "Kế hoạch chức năng - An toàn",
    "CONGVIEC_VAT_TU": "Kế hoạch công việc - Vật tư",
    "CONGVIEC_TRANG_THIET_BI": "Kế hoạch công việc - Trang thiết bị",
    "CONGVIEC_MOI_TRUONG": "Kế hoạch công việc - Môi trường",
    "CONGVIEC_CNTT": "Kế hoạch công việc - CNTT",
}

_KIND_FILTER_MAP = {
    "nhanvien": "NHANVIEN",
    "nhom": "NHOM",
    "khoa": "KHOA",
    "chat_luong": "CHUCNANG_CHAT_LUONG",
    "ky_thuat": "CHUCNANG_KY_THUAT",
    "an_toan": "CHUCNANG_AN_TOAN",
    "vat_tu": "CONGVIEC_VAT_TU",
    "trang_thiet_bi": "CONGVIEC_TRANG_THIET_BI",
    "moi_truong": "CONGVIEC_MOI_TRUONG",
    "cntt": "CONGVIEC_CNTT",
}


def _role_codes_for_user(db: Session, user: Users) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == user.id, isouter=True)
        .join(
            getattr(__import__(__name__), "UserRoles", None) if False else Roles,
            isouter=True
        )
    )
    rows = (
        db.query(Roles.code)
        .join(__import__("app.models", fromlist=["UserRoles"]).UserRoles,
              __import__("app.models", fromlist=["UserRoles"]).UserRoles.role_id == Roles.id)
        .filter(__import__("app.models", fromlist=["UserRoles"]).UserRoles.user_id == user.id)
        .all()
    )
    out: Set[str] = set()
    for (c,) in rows:
        out.add(str(getattr(c, "value", c)).upper())
    return out


def _is_admin(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_ADMIN])


def _is_board(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_LANH_DAO])


def _is_bgd(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_BGD])


def _is_khoa_lead(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [
        RoleCode.ROLE_TRUONG_KHOA,
        RoleCode.ROLE_PHO_TRUONG_KHOA,
        RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    ])


def _same_level_user_ids(db: Session, role_codes: Iterable[str], unit_ids: Iterable[str]) -> List[str]:
    role_codes = [str(x).upper() for x in role_codes or []]
    unit_ids = [str(x) for x in unit_ids or [] if x]
    if not role_codes or not unit_ids:
        return []
    UserRolesCls = __import__("app.models", fromlist=["UserRoles"]).UserRoles
    rows = (
        db.query(UserRolesCls.user_id)
        .join(Roles, UserRolesCls.role_id == Roles.id)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == UserRolesCls.user_id)
        .filter(
            func.upper(func.coalesce(Roles.code, "")).in_(role_codes),
            UserUnitMemberships.unit_id.in_(unit_ids),
        )
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]


def _allowed_creator_kinds(db: Session, user: Users) -> List[Dict[str, str]]:
    role_codes = _role_codes_for_user(db, user)
    opts: List[Dict[str, str]] = [{"value": "NHANVIEN", "label": _PLAN_KIND_LABELS["NHANVIEN"]}]

    if {"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"} & role_codes:
        opts.append({"value": "NHOM", "label": _PLAN_KIND_LABELS["NHOM"]})

    if "ROLE_QL_CHAT_LUONG" in role_codes:
        opts.append({"value": "CHUCNANG_CHAT_LUONG", "label": _PLAN_KIND_LABELS["CHUCNANG_CHAT_LUONG"]})
    if "ROLE_QL_KY_THUAT" in role_codes:
        opts.append({"value": "CHUCNANG_KY_THUAT", "label": _PLAN_KIND_LABELS["CHUCNANG_KY_THUAT"]})
    if "ROLE_QL_AN_TOAN" in role_codes:
        opts.append({"value": "CHUCNANG_AN_TOAN", "label": _PLAN_KIND_LABELS["CHUCNANG_AN_TOAN"]})

    if "ROLE_QL_VAT_TU" in role_codes:
        opts.append({"value": "CONGVIEC_VAT_TU", "label": _PLAN_KIND_LABELS["CONGVIEC_VAT_TU"]})
    if "ROLE_QL_TRANG_THIET_BI" in role_codes:
        opts.append({"value": "CONGVIEC_TRANG_THIET_BI", "label": _PLAN_KIND_LABELS["CONGVIEC_TRANG_THIET_BI"]})
    if "ROLE_QL_MOI_TRUONG" in role_codes:
        opts.append({"value": "CONGVIEC_MOI_TRUONG", "label": _PLAN_KIND_LABELS["CONGVIEC_MOI_TRUONG"]})
    if "ROLE_QL_CNTT" in role_codes or "ROLE_QL_CONG_VIEC" in role_codes:
        opts.append({"value": "CONGVIEC_CNTT", "label": _PLAN_KIND_LABELS["CONGVIEC_CNTT"]})

    if {"ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA", "ROLE_KY_THUAT_VIEN_TRUONG"} & role_codes:
        opts.append({"value": "KHOA", "label": _PLAN_KIND_LABELS["KHOA"]})

    seen = set()
    out: List[Dict[str, str]] = []
    for x in opts:
        if x["value"] in seen:
            continue
        seen.add(x["value"])
        out.append(x)
    return out


def _visible_plan_conditions(db: Session, user: Users) -> List[Any]:
    unit_ids = _matrix_visible_unit_ids(db, user)
    own_group_ids = _group_lead_unit_ids(db, user)
    creator_ids_in_scope = _unit_members_user_ids(db, unit_ids)

    conds: List[Any] = []

    if _is_admin(db, user) or _is_board(db, user) or _is_bgd(db, user):
        khoa_owner_ids = _same_level_user_ids(
            db,
            ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA", "ROLE_KY_THUAT_VIEN_TRUONG"],
            unit_ids,
        )
        conds.append(Plans.plan_kind == "KHOA")
        if khoa_owner_ids:
            conds.append(and_(Plans.plan_kind == "NHANVIEN", Plans.created_by.in_(khoa_owner_ids)))
        return conds

    if _is_khoa_lead(db, user):
        conds.append(Plans.plan_kind == "KHOA")

        # nhìn theo người trong phạm vi quản lý
        if creator_ids_in_scope:
            conds.append(Plans.created_by.in_(creator_ids_in_scope))

        # nhìn theo đơn vị trong phạm vi quản lý
        if unit_ids:
            conds.append(Plans.unit_id.in_(unit_ids))

        # bổ sung chốt theo khoa hiệu lực để tránh lệch do membership seed cũ
        effective_khoa_unit_id = _resolve_effective_khoa_unit_id(db, user)
        if effective_khoa_unit_id:
            conds.append(Plans.unit_id == effective_khoa_unit_id)

        return conds

    if _is_functional_manager(db, user):
        same_level_ids = _same_level_user_ids(
            db,
            ["ROLE_QL_CHAT_LUONG", "ROLE_QL_KY_THUAT", "ROLE_QL_AN_TOAN"],
            unit_ids,
        )
        conds.append(Plans.plan_kind == "KHOA")
        if same_level_ids:
            conds.append(Plans.created_by.in_(same_level_ids))
        if unit_ids:
            conds.append(and_(Plans.unit_id.in_(unit_ids), Plans.plan_kind != "NHANVIEN"))
        return conds

    if _is_operations_manager(db, user):
        same_level_ids = _same_level_user_ids(
            db,
            ["ROLE_QL_VAT_TU", "ROLE_QL_TRANG_THIET_BI", "ROLE_QL_MOI_TRUONG", "ROLE_QL_CNTT", "ROLE_QL_CONG_VIEC"],
            unit_ids,
        )
        conds.append(Plans.plan_kind == "KHOA")
        if same_level_ids:
            conds.append(Plans.created_by.in_(same_level_ids))
        conds.append(Plans.plan_kind.in_(["CHUCNANG_CHAT_LUONG", "CHUCNANG_KY_THUAT", "CHUCNANG_AN_TOAN"]))
        if unit_ids:
            conds.append(and_(Plans.unit_id.in_(unit_ids), Plans.plan_kind != "NHANVIEN"))
        return conds

    if _is_mgr_to(db, user):
        conds.append(Plans.plan_kind == "KHOA")
        if own_group_ids:
            conds.append(and_(Plans.plan_kind == "NHOM", Plans.unit_id.in_(own_group_ids)))
            member_ids = _unit_members_user_ids(db, own_group_ids)
            if member_ids:
                conds.append(and_(Plans.plan_kind == "NHANVIEN", Plans.created_by.in_(member_ids)))
        return conds

    conds.append(Plans.plan_kind == "KHOA")
    my_unit_ids = _user_membership_unit_ids(db, user)
    if my_unit_ids:
        conds.append(and_(Plans.plan_kind == "NHOM", Plans.unit_id.in_(my_unit_ids)))
    conds.append(and_(Plans.plan_kind == "NHANVIEN", Plans.created_by == user.id))
    return conds


async def _notify_plan_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
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
        logger.exception("[plans] Notify realtime lỗi: %s", ex)


def _fire_plan_notify(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    try:
        asyncio.run(_notify_plan_users(user_ids, payload))
    except RuntimeError:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_notify_plan_users(user_ids, payload))
        except Exception as ex:
            logger.exception("[plans] Fire notify lỗi: %s", ex)
        finally:
            try:
                loop.close()
            except Exception:
                pass
                
def _user_primary_units(db: Session, user: Users) -> List[Units]:
    mems = (
        db.query(UserUnitMemberships)
        .filter(
            UserUnitMemberships.user_id == user.id,
            UserUnitMemberships.is_active == True
        )
        .all()
    )

    prims = [m for m in mems if getattr(m, "is_primary", True)]
    ids = [m.unit_id for m in (prims or mems)]
    if not ids:
        return []

    units = (
        db.query(Units)
        .filter(
            Units.id.in_(ids),
            Units.trang_thai == UnitStatus.ACTIVE
        )
        .order_by(Units.cap_do.asc(), Units.order_index.asc())
        .all()
    )

    if units:
        return units

    # fallback cuối cùng nếu DB quá bẩn
    return db.query(Units).filter(Units.id.in_(ids)).all()

def _parent_unit(db: Session, unit_id: str) -> Optional[Units]:
    u = db.get(Units, unit_id)
    if not u or not u.parent_id: return None
    return db.get(Units, u.parent_id)

def _unit_children(db: Session, parent_ids: List[str]) -> List[Units]:
    if not parent_ids: return []
    return db.query(Units).filter(Units.parent_id.in_(parent_ids), Units.trang_thai == UnitStatus.ACTIVE).all()

def _unit_members_user_ids(db: Session, unit_ids: List[str]) -> List[str]:
    if not unit_ids: return []
    rows = db.query(UserUnitMemberships.user_id).filter(UserUnitMemberships.unit_id.in_(unit_ids)).distinct().all()
    return [r[0] for r in rows]

def _user_name_map(db: Session, user_ids: List[str]) -> Dict[str, str]:
    if not user_ids: return {}
    rows = db.query(Users.id, func.coalesce(Users.full_name, Users.username, "")).filter(Users.id.in_(user_ids)).all()
    return {r[0]: r[1] for r in rows}

def _is_mgr_phong(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TRUONG_KHOA, RoleCode.ROLE_PHO_TRUONG_KHOA, RoleCode.ROLE_KY_THUAT_VIEN_TRUONG])

def _is_mgr_to(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO, RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM])

def _is_lab_lead(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_BGD])

def _is_functional_manager(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_QL_CHAT_LUONG, RoleCode.ROLE_QL_KY_THUAT, RoleCode.ROLE_QL_AN_TOAN])

def _is_operations_manager(db: Session, user: Users) -> bool:
    return user_has_any_role(user, db, [RoleCode.ROLE_QL_VAT_TU, RoleCode.ROLE_QL_TRANG_THIET_BI, RoleCode.ROLE_QL_MOI_TRUONG, RoleCode.ROLE_QL_CNTT])

def _managed_unit_ids(db: Session, user: Users) -> List[str]:
    now = datetime.utcnow()
    try:
        rows = db.query(ManagementScopes).filter(ManagementScopes.manager_user_id == user.id).all()
    except Exception:
        return []
    out = []
    for r in rows:
        if getattr(r, 'is_active', True) is False:
            continue
        if getattr(r, 'effective_from', None) and r.effective_from > now:
            continue
        if getattr(r, 'effective_to', None) and r.effective_to < now:
            continue
        tid = getattr(r, 'target_unit_id', None)
        if tid:
            out.append(tid)
    return list(dict.fromkeys(out))

def _group_lead_unit_ids(db: Session, user: Users) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id, Units.cap_do)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user.id)
        .all()
    )
    team_ids = [unit_id for unit_id, cap_do in rows if unit_id and cap_do == 3]
    if team_ids:
        return list(dict.fromkeys(team_ids))
    return [unit_id for unit_id, _cap_do in rows if unit_id]

def _descendant_unit_ids(db: Session, base_ids: List[str]) -> List[str]:
    if not base_ids:
        return []
    seen = set([x for x in base_ids if x])
    pending = list(seen)
    while pending:
        rows = db.query(Units.id).filter(Units.parent_id.in_(pending)).all()
        child_ids = [r[0] for r in rows if r and r[0] and r[0] not in seen]
        if not child_ids:
            break
        seen.update(child_ids)
        pending = child_ids
    return list(seen)

def _matrix_visible_unit_ids(db: Session, user: Users) -> List[str]:
    base_ids = _user_membership_unit_ids(db, user)
    managed_ids = _managed_unit_ids(db, user)

    if is_all_units_access(db, user) or _is_lab_lead(db, user):
        return [r[0] for r in db.query(Units.id).all() if r and r[0]]

    if _is_mgr_phong(db, user):
        return _descendant_unit_ids(db, base_ids)

    if _is_functional_manager(db, user) or _is_operations_manager(db, user):
        return _descendant_unit_ids(db, list(dict.fromkeys(base_ids + managed_ids)))

    if _is_mgr_to(db, user):
        return _group_lead_unit_ids(db, user)

    return base_ids

def _user_membership_unit_ids(db: Session, user: Users) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(
            UserUnitMemberships.user_id == user.id,
            UserUnitMemberships.is_active == True,
            Units.trang_thai == UnitStatus.ACTIVE
        )
        .distinct()
        .all()
    )
    ids = [r[0] for r in rows if r and r[0]]
    if ids:
        return ids

    # fallback nếu DB còn dữ liệu cũ, tránh trắng hoàn toàn
    rows = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user.id)
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]

def _resolve_effective_khoa_unit_id(db: Session, user: Users) -> Optional[str]:
    """
    Ưu tiên tìm đơn vị cấp 2 ACTIVE từ membership hiện hành.
    Nếu user thuộc nhóm (cap_do=3) thì lấy parent ACTIVE cấp 2.
    """
    units = _user_primary_units(db, user)

    # Ưu tiên đơn vị cấp 2 ACTIVE
    for u in units:
        if int(getattr(u, "cap_do", 0) or 0) == 2 and getattr(u, "trang_thai", None) == UnitStatus.ACTIVE:
            return u.id

    # Nếu đang ở nhóm thì lấy khoa cha ACTIVE
    for u in units:
        if int(getattr(u, "cap_do", 0) or 0) == 3 and getattr(u, "parent_id", None):
            parent = db.get(Units, u.parent_id)
            if parent and int(getattr(parent, "cap_do", 0) or 0) == 2 and getattr(parent, "trang_thai", None) == UnitStatus.ACTIVE:
                return parent.id

    return None
    
def _active_visibility_modes(db: Session, user: Users) -> set[str]:
    unit_ids = _user_membership_unit_ids(db, user)
    if not unit_ids:
        return set()

    now = datetime.utcnow()
    grants = (
        db.query(VisibilityGrants)
        .filter(VisibilityGrants.grantee_unit_id.in_(unit_ids))
        .all()
    )

    modes = set()
    for g in grants:
        if g.effective_from and g.effective_from > now:
            continue
        if g.effective_to and g.effective_to < now:
            continue
        mode_val = getattr(g.mode, "value", g.mode)
        if mode_val:
            modes.add(str(mode_val).upper())
    return modes


def _has_plan_visibility_grant(db: Session, user: Users) -> bool:
    modes = _active_visibility_modes(db, user)
    return ("VIEW_ALL" in modes) or ("PLANS_ONLY" in modes)   

def _pad2(n: Optional[str]) -> Optional[str]:
    if not n: return None
    s = str(n).strip()
    if not s.isdigit(): return None
    return s if len(s) == 2 else ("0"+s)[-2:]

def _compose_date(y: Optional[str], m: Optional[str], d: Optional[str]) -> Optional[str]:
    y = (y or "").strip()
    m = _pad2(m); d = _pad2(d)
    if not (y and m and d): return None
    try:
        datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
        return f"{y}-{m}-{d}"
    except Exception:
        return None

def _inject_item_tags(content: str, start: Optional[str], end: Optional[str], status_txt: Optional[str]) -> str:
    parts = []
    if start: parts.append(f"[[START={start}]]")
    if end: parts.append(f"[[END={end}]]")
    if (status_txt or "").strip(): parts.append(f"[[STATUS={(status_txt or '').strip()}]]")
    return ("".join(parts) + " " + (content or "").strip()).strip()

def _extract_period_and_status_from_content(content: str, fallback_due: Optional[datetime]) -> (str, str):
    start, end, st = None, None, ""
    try:
        import re
        m = re.search(r"\[\[START=([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]", content or "");  start = m.group(1) if m else None
        m = re.search(r"\[\[END=([0-9]{4}-[0-9]{2}-[0-9]{2})\]\]", content or "");    end   = m.group(1) if m else None
        m = re.search(r"\[\[STATUS=([^\]]+)\]\]", content or "");                     st    = m.group(1).strip() if m else ""
    except Exception:
        pass

    def _fmt(d):
        try: return datetime.strptime(d, "%Y-%m-%d").strftime("%d-%m-%Y")
        except Exception: return None

    if start or end:
        s = _fmt(start) if start else "?"
        e = _fmt(end) if end else "?"
        period = f"Từ {s or '-'} đến {e or '-'}"
    else:
        period = fallback_due.strftime("%d-%m-%Y") if fallback_due else "-"

    return period, st

def _strip_tags_for_display(content: str) -> str:
    try:
        import re
        return re.sub(r"\[\[(START|END|STATUS)=[^\]]+\]\]\s*", "", content or "").strip()
    except Exception:
        return (content or "").strip()

# ===== Routes =====
@router.get("")
def plans_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    now = datetime.utcnow()

    filter_type = request.query_params.get("filter_type") or ""
    filter_id   = request.query_params.get("filter_id") or ""
    selected_plan_id = request.query_params.get("plan_id") or ""
    selected_kind    = request.query_params.get("kind") or ""

    q_year = (request.query_params.get("year") or "").strip()
    q_month = (request.query_params.get("month") or "").strip()

    y = None
    m = None

    if q_year:
        try:
            y = int(q_year)
        except Exception:
            y = None

    if q_month:
        try:
            m = int(q_month)
        except Exception:
            m = None

    if selected_plan_id and (y is None or m is None):
        try:
            selected_plan = db.get(Plans, selected_plan_id)
            if selected_plan:
                if y is None:
                    y = int(selected_plan.year)
                if m is None:
                    m = int(selected_plan.month)
        except Exception:
            pass

    display_year = y if y is not None else now.year
    display_month = m if m is not None else now.month

    native_all_access = is_all_units_access(db, user)
    grant_all_plan_access = _has_plan_visibility_grant(db, user)

    is_lab_lead = _is_lab_lead(db, user)
    is_mgr_phong = _is_mgr_phong(db, user)
    is_mgr_to = _is_mgr_to(db, user)
    is_func_mgr = _is_functional_manager(db, user)
    is_ops_mgr = _is_operations_manager(db, user)

    can_view_all_plans = native_all_access or grant_all_plan_access or is_lab_lead

    scope_ids = _matrix_visible_unit_ids(db, user)
    visible_units: List[Units] = []
    if scope_ids:
        visible_units = db.query(Units).filter(Units.id.in_(scope_ids)).order_by(Units.cap_do, Units.order_index).all()

    filter_users: List[Dict] = []
    unit_ids = [u.id for u in visible_units]
    uids = [uid for uid in _unit_members_user_ids(db, unit_ids) if uid != user.id]
    name_map = _user_name_map(db, uids)
    filter_users = [{"id": uid, "full_name": name_map.get(uid, "")} for uid in sorted(set(uids))]

    allowed_creator_kinds = _allowed_creator_kinds(db, user)

    q = db.query(Plans)
    if y is not None:
        q = q.filter(Plans.year == y)
    if m is not None:
        q = q.filter(Plans.month == m)

    visible_conds = _visible_plan_conditions(db, user)
    if visible_conds:
        q = q.filter(or_(*visible_conds))
    else:
        q = q.filter(Plans.created_by == user.id)

    if filter_type == "unit" and filter_id:
        q = q.filter(Plans.unit_id == filter_id)
    elif filter_type == "user" and filter_id:
        q = q.filter(Plans.created_by == filter_id)

    mapped_kind = _KIND_FILTER_MAP.get((selected_kind or "").strip().lower())
    if mapped_kind:
        q = q.filter(Plans.plan_kind == mapped_kind)

    is_staff = (not _is_admin(db, user)) and (not _is_board(db, user)) and (not is_lab_lead) and (not is_mgr_phong) and (not is_mgr_to) and (not is_func_mgr) and (not is_ops_mgr)

    plans = q.order_by(Plans.created_at.desc()).all()

    # Gắn items để hiển thị chi tiết
    creator_ids = list({p.created_by for p in plans if p.created_by})
    name_map = _user_name_map(db, creator_ids)
    for p in plans:
        p._plan_kind_label = _PLAN_KIND_LABELS.get((p.plan_kind or "").strip().upper(), (p.plan_kind or "").strip())       
        p._creator_name = name_map.get(p.created_by, "")
        items = db.query(PlanItems).filter(PlanItems.plan_id == p.id).all()
        for it in items:
            vis = _strip_tags_for_display(it.content or "")
            it._content_visible = vis if vis else (it.content or "").strip()
            period, st = _extract_period_and_status_from_content(it.content or "", getattr(it, "due_date", None))
            it._period_label = period
            it._status_label = st
        setattr(p, "items", items)

    create_units: List[Units] = []

    if is_mgr_to:
        # Trưởng/Phó nhóm: cho chọn đúng các nhóm ACTIVE mình thuộc
        for uid in _user_membership_unit_ids(db, user):
            u = db.get(Units, uid)
            if u and int(getattr(u, "cap_do", 0) or 0) == 3 and getattr(u, "trang_thai", None) == UnitStatus.ACTIVE:
                create_units.append(u)
    else:
        # Các vị trí còn lại: chỉ cho chọn Khoa ACTIVE
        khoa_id = _resolve_effective_khoa_unit_id(db, user)
        if khoa_id:
            u = db.get(Units, khoa_id)
            if u:
                create_units.append(u)

    create_units = list({u.id: u for u in create_units}.values())

    filterable_entities = {
        "units": [{"id": u.id, "ten_don_vi": u.ten_don_vi} for u in create_units],
        "users": filter_users
    }

    return templates.TemplateResponse("plans.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "year": display_year,
        "month": display_month,
        "plans": plans,
        "user": user,
        "_is_hdtv": native_all_access or _is_board(db, user),
        "_can_view_all_plans": can_view_all_plans,
        "_is_manager": bool(is_mgr_phong or is_mgr_to or is_lab_lead or is_func_mgr or is_ops_mgr),
        "_is_manager_phong": is_mgr_phong,
        "_is_manager_to": is_mgr_to,
        "_is_staff": is_staff,
        "allowed_creator_kinds": allowed_creator_kinds,
        "plan_kind_labels": _PLAN_KIND_LABELS,
        "_edit_mode": (request.query_params.get("mode") or "") == "edit",
        "filterable_entities": filterable_entities,
        "selected_filter_type": filter_type,
        "selected_filter_id": filter_id,
        "selected_plan_id": request.query_params.get("plan_id") or "",
        "selected_kind": selected_kind,
    })

@router.get("/details/{plan_id}", name="plan_details")

def plan_details(request: Request, plan_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p:
        raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")

    # Cho QL phòng xem kế hoạch của phòng và các tổ trực thuộc (kể cả do nhân sự trong đó tạo)
    native_all_access = is_all_units_access(db, user)
    grant_all_plan_access = _has_plan_visibility_grant(db, user)
    is_lab_lead = _is_lab_lead(db, user)

    visible_conds = _visible_plan_conditions(db, user)
    q_check = db.query(Plans).filter(Plans.id == plan_id)
    if visible_conds:
        q_check = q_check.filter(or_(*visible_conds))
    p = q_check.first()

    if not p:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền xem kế hoạch này.")

    items = db.query(PlanItems).filter(PlanItems.plan_id == plan_id).all()
    for it in items:
        vis = _strip_tags_for_display(it.content or "")
        it._content_visible = vis if vis else (it.content or "").strip()
        period, st = _extract_period_and_status_from_content(it.content or "", getattr(it, "due_date", None))
        it._period_label = period
        it._status_label = st
    p.items = items

    p._creator_name = db.query(func.coalesce(Users.full_name, Users.username, "")).filter(Users.id == p.created_by).scalar() or ""
    p._plan_kind_label = _PLAN_KIND_LABELS.get((p.plan_kind or "").strip().upper(), (p.plan_kind or "").strip())
    
    return templates.TemplateResponse("plans.html", {
        "request": request,
        "year": p.year,
        "month": p.month,
        "plans": [p],
        "user": user,
        "_is_hdtv": native_all_access,
        "_can_view_all_plans": (native_all_access or grant_all_plan_access),
        "_is_manager": False,
        "_is_manager_phong": False,
        "_is_manager_to": False,
        "_is_staff": False,
        "_edit_mode": (request.query_params.get("mode") or "") == "edit",
        "filterable_entities": {"units": [], "users": []},
        "selected_filter_type": "",
        "selected_filter_id": "",
        "selected_plan_id": plan_id,
        "selected_kind": "",
        "allowed_creator_kinds": _allowed_creator_kinds(db, user),
        "plan_kind_labels": _PLAN_KIND_LABELS,        
    })

@router.post("/create", name="add_plan")
def create_plan(
    request: Request,
    title: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    description: str = Form(""),
    unit_id: str = Form(...),

    # NHẬN CẢ HAI KIỂU TÊN TRƯỜNG: có [] và không có []
    item_contents: Optional[List[str]] = Form(None),
    item_contents2: Optional[List[str]] = Form(None, alias="item_contents[]"),

    item_start_y: Optional[List[str]] = Form(None),
    item_start_y2: Optional[List[str]] = Form(None, alias="item_start_y[]"),
    item_start_m: Optional[List[str]] = Form(None),
    item_start_m2: Optional[List[str]] = Form(None, alias="item_start_m[]"),
    item_start_d: Optional[List[str]] = Form(None),
    item_start_d2: Optional[List[str]] = Form(None, alias="item_start_d[]"),

    item_end_y: Optional[List[str]] = Form(None),
    item_end_y2: Optional[List[str]] = Form(None, alias="item_end_y[]"),
    item_end_m: Optional[List[str]] = Form(None),
    item_end_m2: Optional[List[str]] = Form(None, alias="item_end_m[]"),
    item_end_d: Optional[List[str]] = Form(None),
    item_end_d2: Optional[List[str]] = Form(None, alias="item_end_d[]"),

    item_statuses: Optional[List[str]] = Form(None),
    item_statuses2: Optional[List[str]] = Form(None, alias="item_statuses[]"),

    creator_kind: str = Form(""),

    _secret_check: Users = Depends(
        require_secret_lock(ActionCode.ASSIGN_TASK_DOWNSTREAM) if _ENABLE_SECRET_LOCK_PLANS else _skip_secret_lock
    ),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)

    creator_kind_up = (creator_kind or "").strip().upper()
    allowed_kind_values = [x["value"] for x in _allowed_creator_kinds(db, user)]
    if creator_kind_up not in allowed_kind_values:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Không có quyền lập loại kế hoạch này.")

    effective_khoa_unit_id = _resolve_effective_khoa_unit_id(db, user)

    # Ép đơn vị theo loại kế hoạch
    forced_unit_id = unit_id
    if creator_kind_up in {
        "KHOA",
        "CHUCNANG_CHAT_LUONG",
        "CHUCNANG_KY_THUAT",
        "CHUCNANG_AN_TOAN",
        "CONGVIEC_VAT_TU",
        "CONGVIEC_TRANG_THIET_BI",
        "CONGVIEC_MOI_TRUONG",
        "CONGVIEC_CNTT",
    }:
        if not effective_khoa_unit_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Không xác định được đơn vị Khoa hiệu lực để lập kế hoạch.")
        forced_unit_id = effective_khoa_unit_id

    elif creator_kind_up == "NHOM":
        # Kế hoạch nhóm chỉ được gắn vào đơn vị cấp 3 ACTIVE mà user đang thuộc
        allowed_group_ids = []
        for uid in _user_membership_unit_ids(db, user):
            u = db.get(Units, uid)
            if u and int(getattr(u, "cap_do", 0) or 0) == 3 and getattr(u, "trang_thai", None) == UnitStatus.ACTIVE:
                allowed_group_ids.append(u.id)

        allowed_group_ids = list(dict.fromkeys(allowed_group_ids))
        if unit_id not in allowed_group_ids:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Không có quyền lập kế hoạch nhóm tại đơn vị này.")
        forced_unit_id = unit_id

    elif creator_kind_up == "NHANVIEN":
        # Kế hoạch cá nhân: ưu tiên gắn khoa ACTIVE của user
        if effective_khoa_unit_id:
            forced_unit_id = effective_khoa_unit_id

    allowed_unit_ids = set(_matrix_visible_unit_ids(db, user))
    can_create_matrix = (
        _is_lab_lead(db, user)
        or _is_mgr_phong(db, user)
        or _is_mgr_to(db, user)
        or _is_functional_manager(db, user)
        or _is_operations_manager(db, user)
    )

    if not (
        is_all_units_access(db, user)
        or forced_unit_id in allowed_unit_ids
        or (not can_create_matrix and forced_unit_id in _user_membership_unit_ids(db, user))
    ):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Không có quyền tạo kế hoạch tại đơn vị này.")

    unit_id = forced_unit_id

    p = Plans(
        title=(title or "").strip(),
        year=int(year),
        month=int(month),
        description=(description or "").strip(),
        plan_kind=creator_kind_up,
        unit_id=unit_id,
        status=PlanStatus.DRAFT,
        created_by=user.id,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(p); db.commit(); db.refresh(p)

    # Chọn nguồn dữ liệu: ưu tiên field có []
    contents = item_contents2 or item_contents
    sy_list  = item_start_y2 or item_start_y
    sm_list  = item_start_m2 or item_start_m
    sd_list  = item_start_d2 or item_start_d
    ey_list  = item_end_y2   or item_end_y
    em_list  = item_end_m2   or item_end_m
    ed_list  = item_end_d2   or item_end_d
    st_list  = item_statuses2 or item_statuses

    if contents:
        n = len(contents)
        def _get(lst, i): return (lst[i] if lst and i < len(lst) else None)

        for i in range(n):
            content = (_get(contents, i) or "").strip()

            sy = _get(sy_list, i) or str(year)
            ey = _get(ey_list, i) or str(year)
            sm = _get(sm_list, i); sd = _get(sd_list, i)
            em = _get(em_list, i); ed = _get(ed_list, i)

            start = _compose_date(sy, sm, sd)
            end   = _compose_date(ey, em, ed)
            stxt  = _get(st_list, i)

            # Bỏ dòng trống hoàn toàn
            if not content and not start and not end and not stxt:
                continue

            it = PlanItems(
                plan_id=p.id,
                content=_inject_item_tags(content, start, end, stxt),
                due_date=datetime.strptime(end, "%Y-%m-%d") if end else None
            )
            db.add(it)
            
        db.commit()

    notify_user_ids = _unit_members_user_ids(db, _matrix_visible_unit_ids(db, user))
    _fire_plan_notify(
        notify_user_ids,
        {
            "module": "plans",
            "type": "plan_created",
            "plan_id": str(p.id),
            "title": p.title or "",
            "timestamp": datetime.utcnow().isoformat(),
        },
    )

    return RedirectResponse(
        url=f"/plans?year={p.year}&month={p.month}&plan_id={p.id}",
        status_code=302,
    )

@router.post("/update", name="update_plan")
def update_plan(
    request: Request,
    plan_id: str = Form(...),
    title: str = Form(...),
    year: int = Form(...),
    month: int = Form(...),
    description: str = Form(""),

    # NHẬN CẢ HAI KIỂU TÊN TRƯỜNG: có [] và không có []
    item_contents: Optional[List[str]] = Form(None),
    item_contents2: Optional[List[str]] = Form(None, alias="item_contents[]"),

    item_start_y: Optional[List[str]] = Form(None),
    item_start_y2: Optional[List[str]] = Form(None, alias="item_start_y[]"),
    item_start_m: Optional[List[str]] = Form(None),
    item_start_m2: Optional[List[str]] = Form(None, alias="item_start_m[]"),
    item_start_d: Optional[List[str]] = Form(None),
    item_start_d2: Optional[List[str]] = Form(None, alias="item_start_d[]"),

    item_end_y: Optional[List[str]] = Form(None),
    item_end_y2: Optional[List[str]] = Form(None, alias="item_end_y[]"),
    item_end_m: Optional[List[str]] = Form(None),
    item_end_m2: Optional[List[str]] = Form(None, alias="item_end_m[]"),
    item_end_d: Optional[List[str]] = Form(None),
    item_end_d2: Optional[List[str]] = Form(None, alias="item_end_d[]"),

    item_statuses: Optional[List[str]] = Form(None),
    item_statuses2: Optional[List[str]] = Form(None, alias="item_statuses[]"),

    _secret_check: Users = Depends(
        require_secret_lock(ActionCode.ASSIGN_TASK_DOWNSTREAM) if _ENABLE_SECRET_LOCK_PLANS else _skip_secret_lock
    ),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p:
        raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")
    allowed_unit_ids = set(_matrix_visible_unit_ids(db, user))
    can_manage_scope = _is_lab_lead(db, user) or _is_mgr_phong(db, user) or _is_functional_manager(db, user) or _is_operations_manager(db, user)
    if p.created_by != user.id and not (can_manage_scope and (p.unit_id in allowed_unit_ids)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền cập nhật kế hoạch này.")

    p.title = (title or "").strip()
    p.year = int(year)
    p.month = int(month)
    p.description = (description or "").strip()
    p.updated_at = datetime.utcnow()
    db.add(p)

    # Chọn nguồn dữ liệu: ưu tiên field có []
    contents = item_contents2 or item_contents
    sy_list  = item_start_y2 or item_start_y
    sm_list  = item_start_m2 or item_start_m
    sd_list  = item_start_d2 or item_start_d
    ey_list  = item_end_y2   or item_end_y
    em_list  = item_end_m2   or item_end_m
    ed_list  = item_end_d2   or item_end_d
    st_list  = item_statuses2 or item_statuses

    def _get(lst, i):
        return (lst[i] if lst and i < len(lst) else None)

    # Luôn đồng bộ lại toàn bộ dòng công việc theo dữ liệu form hiện tại
    db.query(PlanItems).filter(PlanItems.plan_id == p.id).delete(synchronize_session=False)

    if contents:
        n = len(contents)
        for i in range(n):
            content = (_get(contents, i) or "").strip()

            sy = _get(sy_list, i) or str(year)
            ey = _get(ey_list, i) or str(year)
            sm = _get(sm_list, i)
            sd = _get(sd_list, i)
            em = _get(em_list, i)
            ed = _get(ed_list, i)

            start = _compose_date(sy, sm, sd)
            end   = _compose_date(ey, em, ed)
            stxt  = _get(st_list, i)

            # Bỏ dòng trống hoàn toàn
            if not content and not start and not end and not stxt:
                continue

            it = PlanItems(
                plan_id=p.id,
                content=_inject_item_tags(content, start, end, stxt),
                due_date=datetime.strptime(end, "%Y-%m-%d") if end else None
            )
            db.add(it)

    db.commit()
    notify_user_ids = _unit_members_user_ids(db, _matrix_visible_unit_ids(db, user))
    _fire_plan_notify(
        notify_user_ids,
        {
            "module": "plans",
            "type": "plan_updated",
            "plan_id": str(p.id),
            "title": p.title or "",
            "timestamp": datetime.utcnow().isoformat(),
        },
    )    
    return RedirectResponse(
        url=f"/plans?year={p.year}&month={p.month}&plan_id={p.id}",
        status_code=302,
    )

@router.post("/delete")
def delete_plan(request: Request, plan_id: str = Form(...), db: Session = Depends(get_db)):
    user = login_required(request, db)
    p = db.get(Plans, plan_id)
    if not p: raise HTTPException(status_code=404, detail="Không tìm thấy kế hoạch.")
    allowed_unit_ids = set(_matrix_visible_unit_ids(db, user))
    can_manage_scope = _is_lab_lead(db, user) or _is_mgr_phong(db, user)
    if p.created_by != user.id and not (can_manage_scope and (p.unit_id in allowed_unit_ids)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Bạn không có quyền xóa kế hoạch này.")
    y, m = p.year, p.month
    db.query(PlanItems).filter(PlanItems.plan_id == plan_id).delete(synchronize_session=False)
    plan_title = p.title or ""
    notify_user_ids = _unit_members_user_ids(db, _matrix_visible_unit_ids(db, user))    
    db.delete(p)
    db.commit()

    _fire_plan_notify(
        notify_user_ids,
        {
            "module": "plans",
            "type": "plan_deleted",
            "plan_id": str(plan_id),
            "title": plan_title,
            "timestamp": datetime.utcnow().isoformat(),
        },
    )

    return RedirectResponse(url=f"/plans?year={y}&month={m}", status_code=302)
