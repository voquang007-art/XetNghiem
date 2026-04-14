from __future__ import annotations

import os
import shutil
import uuid
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..chat.realtime import manager
from ..config import settings
from ..database import Base, engine, get_db
from ..models import (
    DocumentDraftActions,
    DocumentDraftFiles,
    DocumentDrafts,
    Files,
    ManagementScopes,
    ReportingLines,
    RoleCode,
    Roles,
    ScopePermissions,
    UnitStatus,
    Units,
    UserRoles,
    UserStatus,
    UserUnitMemberships,
    Users,
)
from ..security.deps import login_required

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_ALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".webp", ".txt"
}
_INLINE_MIME_PREFIXES = ("image/", "text/")
_INLINE_MIME_EXACT = {
    "application/pdf",
}
_COORDINATION_ACTION = "COORDINATE"

_STATUS_LABELS = {
    "DRAFT": "Nháp",
    "RETURNED_FOR_EDIT": "Trả lại để chỉnh sửa",
    "SUBMITTED_TO_TO_MANAGER": "Chờ QL tổ xử lý",
    "SUBMITTED_TO_DEPT_MANAGER": "Chờ QL phòng xử lý",
    "SUBMITTED_TO_HDTV": "Chờ HĐTV phê duyệt",
    "IN_COORDINATION": "Đang phối hợp",
    "FINISHED": "Đã kết thúc",
}

_ACTION_LABELS = {
    "CREATE": "Tạo hồ sơ",
    "SUBMIT": "Trình dự thảo",
    "UPLOAD_REPLACEMENT": "Cập nhật tài liệu dự thảo",
    "COORDINATE": "Gửi phối hợp",
    "COORDINATE_REPLY": "Phản hồi phối hợp",
    "APPROVE_FORWARD": "Đồng ý và trình cấp trên",
    "RETURN_FOR_EDIT": "Trả lại để tự sửa",
    "RETURN_WITH_EDITED_FILE": "Trả lại kèm file đã sửa",
    "HDTV_APPROVED": "HĐTV phê duyệt nội dung",
    "FINISHED": "Kết thúc hồ sơ",
}

_FILE_ROLE_LABELS = {
    "DRAFT_UPLOAD": "Tài liệu dự thảo",
    "RETURNED_EDITED_FILE": "Tài liệu trả lại đã sửa",
}

def _ensure_tables() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            DocumentDrafts.__table__,
            DocumentDraftFiles.__table__,
            DocumentDraftActions.__table__,
        ],
        checkfirst=True,
    )


def _now() -> datetime:
    return datetime.utcnow()


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _upload_root() -> str:
    path = os.path.join(_project_root(), "data", "draft_approvals")
    os.makedirs(path, exist_ok=True)
    return path


def _normalize_role_code(value: object) -> str:
    if value is None:
        return ""
    raw = getattr(value, "value", value)
    return str(raw).strip().upper()


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    return {_normalize_role_code(code) for (code,) in rows}


def _is_admin(role_codes: Set[str]) -> bool:
    return "ROLE_ADMIN" in role_codes


def _is_board(role_codes: Set[str]) -> bool:
    return "ROLE_LANH_DAO" in role_codes


def _is_bgd(role_codes: Set[str]) -> bool:
    return "ROLE_BGD" in role_codes


def _is_team_manager(role_codes: Set[str]) -> bool:
    return bool({"ROLE_TO_TRUONG", "ROLE_PHO_TO", "ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"} & role_codes)


def _is_truong_khoa(role_codes: Set[str]) -> bool:
    return "ROLE_TRUONG_KHOA" in role_codes


def _is_pho_khoa(role_codes: Set[str]) -> bool:
    return "ROLE_PHO_TRUONG_KHOA" in role_codes


def _is_ktv_truong(role_codes: Set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG" in role_codes


def _is_ql_cong_viec(role_codes: Set[str]) -> bool:
    return "ROLE_QL_CONG_VIEC" in role_codes


def _is_functional_manager(role_codes: Set[str]) -> bool:
    return bool({
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
    } & role_codes)


def _is_operations_manager(role_codes: Set[str]) -> bool:
    return bool({
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
    } & role_codes)


def _is_matrix_manager(role_codes: Set[str]) -> bool:
    return _is_ktv_truong(role_codes) or _is_ql_cong_viec(role_codes) or _is_functional_manager(role_codes) or _is_operations_manager(role_codes)


def _is_lab_lead(role_codes: Set[str]) -> bool:
    return _is_truong_khoa(role_codes) or _is_pho_khoa(role_codes)


def _is_employee(role_codes: Set[str]) -> bool:
    return "ROLE_NHAN_VIEN" in role_codes


def _get_primary_membership(db: Session, user_id: str) -> Optional[UserUnitMemberships]:
    membership = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(UserUnitMemberships.is_primary.desc(), UserUnitMemberships.unit_id.asc())
        .first()
    )
    return membership


def _get_membership_units(db: Session, user_id: str) -> List[Units]:
    return (
        db.query(Units)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )


def _group_lead_unit_ids(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Units.id, Units.cap_do)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )
    team_ids = {str(unit_id) for unit_id, cap_do in rows if unit_id and cap_do == 3}
    if team_ids:
        return team_ids
    return {str(unit_id) for unit_id, _cap_do in rows if unit_id}


def _descendant_unit_ids(db: Session, root_ids: Set[str]) -> Set[str]:
    if not root_ids:
        return set()
    seen = set(str(x) for x in root_ids if x)
    queue = list(seen)
    while queue:
        current = queue.pop(0)
        rows = db.query(Units.id).filter(Units.parent_id == current).all()
        for (child_id,) in rows:
            if child_id and child_id not in seen:
                seen.add(child_id)
                queue.append(child_id)
    return seen


def _managed_scope_unit_ids(db: Session, user_id: str, permission_code: Optional[str] = None) -> Set[str]:
    now = _now()
    rows = db.query(ManagementScopes).filter(ManagementScopes.manager_user_id == user_id, ManagementScopes.is_active.is_(True)).all()
    scope_ids = []
    unit_ids: Set[str] = set()
    for row in rows:
        if getattr(row, 'effective_from', None) and row.effective_from > now:
            continue
        if getattr(row, 'effective_to', None) and row.effective_to < now:
            continue
        if getattr(row, 'target_unit_id', None):
            unit_ids.add(str(row.target_unit_id))
            scope_ids.append(row.id)
    if permission_code and scope_ids:
        allowed_scope_ids = {sid for (sid,) in db.query(ScopePermissions.scope_id).filter(ScopePermissions.scope_id.in_(scope_ids), ScopePermissions.permission_code == permission_code).all()}
        unit_ids = {str(row.target_unit_id) for row in rows if row.id in allowed_scope_ids and getattr(row, 'target_unit_id', None)}
    return _descendant_unit_ids(db, unit_ids)


def _find_reporting_target(db: Session, user_id: str) -> Optional[Users]:
    line = (
        db.query(ReportingLines)
        .filter(ReportingLines.from_user_id == user_id, ReportingLines.is_active.is_(True))
        .order_by(ReportingLines.priority_no.asc(), ReportingLines.created_at.asc())
        .first()
    )
    if not line or not getattr(line, 'to_user_id', None):
        return None
    return db.get(Users, line.to_user_id)


def _find_users_by_roles(db: Session, role_codes: List[RoleCode], unit_ids: Optional[Set[str]] = None, exclude_user_id: Optional[str] = None) -> List[Users]:
    q = (
        db.query(Users)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(Users.status == UserStatus.ACTIVE, Roles.code.in_(role_codes))
    )
    if unit_ids:
        q = q.join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id).filter(UserUnitMemberships.unit_id.in_(list(unit_ids)))
    if exclude_user_id:
        q = q.filter(Users.id != exclude_user_id)
    rows = q.order_by(Users.full_name.asc(), Users.username.asc()).all()
    unique = {}
    for row in rows:
        unique[row.id] = row
    return list(unique.values())


def _find_bgd_user(db: Session) -> Optional[Users]:
    rows = _find_users_by_roles(db, [RoleCode.ROLE_BGD])
    return rows[0] if rows else None


def _find_lab_lead_user(db: Session, preferred_unit: Optional[Units] = None) -> Optional[Users]:
    preferred_ids: Set[str] = set()
    if preferred_unit and getattr(preferred_unit, 'id', None):
        preferred_ids.add(preferred_unit.id)
        if getattr(preferred_unit, 'parent_id', None):
            preferred_ids.add(preferred_unit.parent_id)
    rows = _find_users_by_roles(db, [RoleCode.ROLE_TRUONG_KHOA, RoleCode.ROLE_PHO_TRUONG_KHOA, RoleCode.ROLE_KY_THUAT_VIEN_TRUONG], preferred_ids or None)
    if rows:
        return rows[0]
    rows = _find_users_by_roles(db, [RoleCode.ROLE_TRUONG_KHOA, RoleCode.ROLE_PHO_TRUONG_KHOA, RoleCode.ROLE_KY_THUAT_VIEN_TRUONG])
    return rows[0] if rows else None


def _find_matrix_manager_for_unit(db: Session, unit: Optional[Units], exclude_user_id: Optional[str] = None) -> Optional[Users]:
    if not unit or not getattr(unit, 'id', None):
        return None
    manager_ids = [row[0] for row in db.query(ManagementScopes.manager_user_id).filter(ManagementScopes.target_unit_id == unit.id, ManagementScopes.is_active.is_(True)).distinct().all()]
    if not manager_ids:
        return None
    rows = _find_users_by_roles(
        db,
        [RoleCode.ROLE_KY_THUAT_VIEN_TRUONG, RoleCode.ROLE_QL_CHAT_LUONG, RoleCode.ROLE_QL_KY_THUAT, RoleCode.ROLE_QL_AN_TOAN, RoleCode.ROLE_QL_VAT_TU, RoleCode.ROLE_QL_TRANG_THIET_BI, RoleCode.ROLE_QL_MOI_TRUONG, RoleCode.ROLE_QL_CNTT, RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM],
        exclude_user_id=exclude_user_id,
    )
    rows = [u for u in rows if u.id in set(manager_ids)]
    return rows[0] if rows else None


def _get_accessible_unit_ids(db: Session, user: Users, role_codes: Set[str]) -> Set[str]:
    member_units = _get_membership_units(db, user.id)
    member_ids = {u.id for u in member_units}
    team_member_ids = _group_lead_unit_ids(db, user.id)
    if _is_board(role_codes):
        return {row[0] for row in db.query(Units.id).all()}
    if _is_lab_lead(role_codes) or _is_ktv_truong(role_codes):
        return {row[0] for row in db.query(Units.id).all()}
    scope_ids = _managed_scope_unit_ids(db, user.id, None) if _is_matrix_manager(role_codes) else set()
    if _is_team_manager(role_codes):
        return team_member_ids or member_ids
    if _is_functional_manager(role_codes) or _is_operations_manager(role_codes):
        return member_ids | scope_ids
    return member_ids


def _get_unit(db: Session, unit_id: Optional[str]) -> Optional[Units]:
    if not unit_id:
        return None
    return db.get(Units, unit_id)


def _unit_label(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return getattr(unit, "ten_don_vi", None) or unit.id


def _status_label(value: Optional[str]) -> str:
    return _STATUS_LABELS.get((value or '').strip(), value or '—')


def _action_label(value: Optional[str]) -> str:
    return _ACTION_LABELS.get((value or '').strip(), value or '—')


def _file_role_label(value: Optional[str]) -> str:
    return _FILE_ROLE_LABELS.get((value or '').strip(), value or 'Tài liệu')


def _display_role_label(role_codes: Set[str]) -> str:
    if _is_board(role_codes):
        return "HĐTV"
    if _is_bgd(role_codes):
        return "BGĐ"
    if _is_truong_khoa(role_codes):
        return "Trưởng khoa"
    if _is_pho_khoa(role_codes):
        return "Phó khoa"
    if _is_ktv_truong(role_codes):
        return "Kỹ thuật viên trưởng"
    if _is_ql_cong_viec(role_codes):
        return "QL công việc"
    if "ROLE_QL_CHAT_LUONG" in role_codes:
        return "QL chất lượng"
    if "ROLE_QL_KY_THUAT" in role_codes:
        return "QL kỹ thuật"
    if "ROLE_QL_AN_TOAN" in role_codes:
        return "QL an toàn"
    if "ROLE_QL_VAT_TU" in role_codes:
        return "QL vật tư"
    if "ROLE_QL_TRANG_THIET_BI" in role_codes:
        return "QL trang thiết bị"
    if "ROLE_QL_MOI_TRUONG" in role_codes:
        return "QL môi trường"
    if "ROLE_QL_CNTT" in role_codes:
        return "QL CNTT"
    if "ROLE_TRUONG_NHOM" in role_codes or "ROLE_TO_TRUONG" in role_codes:
        return "Nhóm/Tổ trưởng"
    if "ROLE_PHO_NHOM" in role_codes or "ROLE_PHO_TO" in role_codes:
        return "Nhóm/Tổ phó"
    if _is_employee(role_codes):
        return "Nhân viên"
    return "Người dùng"


def _display_user_option_label(user: Users, role_codes: Set[str], unit: Optional[Units]) -> str:
    name = user.full_name or user.username or user.id
    role_label = _display_role_label(role_codes)

    # Chỉ hiển thị tên đơn vị với các vị trí thuộc đơn vị cấp 3:
    # Nhóm/Tổ trưởng, Nhóm/Tổ phó, Nhân viên
    if unit and getattr(unit, "cap_do", None) == 3:
        if (
            "ROLE_TRUONG_NHOM" in role_codes
            or "ROLE_TO_TRUONG" in role_codes
            or "ROLE_PHO_NHOM" in role_codes
            or "ROLE_PHO_TO" in role_codes
            or _is_employee(role_codes)
        ):
            return f"{name} — {role_label} — {unit.ten_don_vi}"

    return f"{name} — {role_label}"


async def _notify_draft_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    """
    Realtime cho module draft approval.
    Dùng await trực tiếp, tránh lặp lại lỗi from_thread.run như task/inbox trước đó.
    """
    clean_ids: List[str] = []
    for raw_user_id in user_ids:
        uid = str(raw_user_id or "").strip()
        if uid and uid not in clean_ids:
            clean_ids.append(uid)

    if not clean_ids:
        return

    await manager.notify_users_json(clean_ids, payload)


def _user_label(user: Optional[Users]) -> str:
    if not user:
        return ""
    return user.full_name or user.username or user.id

def _safe_filename(filename: str) -> str:
    return Path(filename or "file").name.replace("..", "_")


def _is_allowed_file(filename: str) -> bool:
    return Path(filename or "").suffix.lower() in _ALLOWED_EXTENSIONS


def _save_upload(upload: UploadFile, draft_id: str) -> Tuple[str, int, str]:
    original_name = _safe_filename(upload.filename or "tep_dinh_kem")
    ext = Path(original_name).suffix.lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Định dạng tệp không được hỗ trợ.")
    folder = os.path.join(_upload_root(), draft_id)
    os.makedirs(folder, exist_ok=True)
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}{ext}"
    dest = os.path.join(folder, stored_name)
    with open(dest, "wb") as fh:
        shutil.copyfileobj(upload.file, fh)
    size = os.path.getsize(dest)
    mime_type = upload.content_type or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
    return dest, size, mime_type


def _deactivate_active_files(db: Session, draft_id: str) -> None:
    rows = (
        db.query(DocumentDraftFiles)
        .filter(DocumentDraftFiles.draft_id == draft_id, DocumentDraftFiles.is_deleted.is_(False), DocumentDraftFiles.is_active.is_(True))
        .all()
    )
    for row in rows:
        row.is_active = False
        db.add(row)


def _add_file_record(
    db: Session,
    draft: DocumentDrafts,
    upload: UploadFile,
    uploaded_by: str,
    file_role: str,
    activate: bool = True,
) -> DocumentDraftFiles:
    if activate:
        _deactivate_active_files(db, draft.id)
    path, size, mime_type = _save_upload(upload, draft.id)
    rec = DocumentDraftFiles(
        draft_id=draft.id,
        file_name=_safe_filename(upload.filename or "tep_dinh_kem"),
        file_path=path,
        mime_type=mime_type,
        size_bytes=size,
        file_role=file_role,
        uploaded_by=uploaded_by,
        is_active=activate,
        is_deleted=False,
    )
    db.add(rec)
    db.flush()
    return rec


def _log_action(
    db: Session,
    draft: DocumentDrafts,
    action_type: str,
    from_user_id: Optional[str] = None,
    to_user_id: Optional[str] = None,
    from_unit_id: Optional[str] = None,
    to_unit_id: Optional[str] = None,
    comment: str = "",
    linked_file_id: Optional[str] = None,
    is_pending: bool = False,
    response_text: Optional[str] = None,
    responded_at: Optional[datetime] = None,
) -> DocumentDraftActions:
    action = DocumentDraftActions(
        draft_id=draft.id,
        action_type=action_type,
        from_user_id=from_user_id,
        to_user_id=to_user_id,
        from_unit_id=from_unit_id,
        to_unit_id=to_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=linked_file_id,
        is_pending=is_pending,
        response_text=response_text,
        responded_at=responded_at,
    )
    db.add(action)
    db.flush()
    return action


def _active_file(db: Session, draft_id: str) -> Optional[DocumentDraftFiles]:
    return (
        db.query(DocumentDraftFiles)
        .filter(
            DocumentDraftFiles.draft_id == draft_id,
            DocumentDraftFiles.is_deleted.is_(False),
            DocumentDraftFiles.is_active.is_(True),
        )
        .order_by(DocumentDraftFiles.uploaded_at.desc())
        .first()
    )


def _find_team_manager(db: Session, team_unit_id: str) -> Optional[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == team_unit_id,
            Roles.code.in_([RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO, RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows[0] if rows else None


def _find_room_manager(db: Session, room_unit_id: str) -> Optional[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == room_unit_id,
            Roles.code.in_([RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows[0] if rows else None


def _find_board_user(db: Session) -> Optional[Users]:
    rows = _find_users_by_roles(db, [RoleCode.ROLE_LANH_DAO])
    return rows[0] if rows else None


def _find_submit_target(
    db: Session,
    user: Users,
    role_codes: Set[str],
    primary_unit: Optional[Units],
) -> Tuple[Optional[Users], Optional[Units], str]:
    """
    Tuyến trình Xét nghiệm:
    - Nhân viên -> Nhóm trưởng / Nhóm phó của nhóm mình
    - Nhóm trưởng / Nhóm phó -> QL công việc / QL chức năng / KTV trưởng
    - QL công việc -> QL chức năng / KTV trưởng
    - QL chức năng -> KTV trưởng / Phó khoa / Trưởng khoa
    - KTV trưởng -> Phó khoa / Trưởng khoa
    - Phó khoa -> Trưởng khoa / BGĐ / HĐTV
    - Trưởng khoa -> BGĐ / HĐTV
    - BGĐ -> HĐTV
    - Admin: chỉ xem, không tham gia
    """
    if _is_admin(role_codes):
        return None, None, "Admin chỉ được xem hồ sơ, không tham gia trình duyệt."

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else primary_unit

    khoa_unit = None
    if primary_unit:
        if getattr(primary_unit, "cap_do", None) == 2:
            khoa_unit = primary_unit
        elif getattr(primary_unit, "parent_id", None):
            khoa_unit = db.get(Units, primary_unit.parent_id)

    def _pick_first(role_list: List[RoleCode], unit_ids: Optional[Set[str]] = None) -> Tuple[Optional[Users], Optional[Units]]:
        rows = _find_users_by_roles(db, role_list, unit_ids, exclude_user_id=user.id)
        if not rows:
            return None, None
        target_user = rows[0]
        target_membership = _get_primary_membership(db, target_user.id)
        target_unit = db.get(Units, target_membership.unit_id) if target_membership and target_membership.unit_id else None
        return target_user, target_unit

    khoa_unit_ids = {khoa_unit.id} if khoa_unit and getattr(khoa_unit, "id", None) else None
    same_team_ids = {primary_unit.id} if primary_unit and getattr(primary_unit, "cap_do", None) == 3 else None

    if _is_employee(role_codes):
        target_user, target_unit = _pick_first(
            [RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM, RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO],
            same_team_ids,
        )
        if target_user:
            return target_user, target_unit or primary_unit, "SUBMITTED_TO_TO_MANAGER"
        return None, None, "Không tìm thấy Nhóm trưởng/Nhóm phó của nhóm hiện tại."

    if _is_team_manager(role_codes):
        target_user, target_unit = _pick_first(
            [
                RoleCode.ROLE_QL_CONG_VIEC,
                RoleCode.ROLE_QL_CHAT_LUONG,
                RoleCode.ROLE_QL_KY_THUAT,
                RoleCode.ROLE_QL_AN_TOAN,
                RoleCode.ROLE_QL_VAT_TU,
                RoleCode.ROLE_QL_TRANG_THIET_BI,
                RoleCode.ROLE_QL_MOI_TRUONG,
                RoleCode.ROLE_QL_CNTT,
                RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
            ],
            khoa_unit_ids,
        )
        if target_user:
            return target_user, target_unit, "SUBMITTED_TO_TO_MANAGER"
        return None, None, "Không tìm thấy QL công việc / QL chức năng / Kỹ thuật viên trưởng."

    if _is_ql_cong_viec(role_codes):
        target_user, target_unit = _pick_first(
            [
                RoleCode.ROLE_QL_CHAT_LUONG,
                RoleCode.ROLE_QL_KY_THUAT,
                RoleCode.ROLE_QL_AN_TOAN,
                RoleCode.ROLE_QL_VAT_TU,
                RoleCode.ROLE_QL_TRANG_THIET_BI,
                RoleCode.ROLE_QL_MOI_TRUONG,
                RoleCode.ROLE_QL_CNTT,
                RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
            ],
            khoa_unit_ids,
        )
        if target_user:
            return target_user, target_unit, "SUBMITTED_TO_TO_MANAGER"
        return None, None, "Không tìm thấy QL chức năng / Kỹ thuật viên trưởng."

    if _is_functional_manager(role_codes):
        target_user, target_unit = _pick_first(
            [
                RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
                RoleCode.ROLE_PHO_TRUONG_KHOA,
                RoleCode.ROLE_TRUONG_KHOA,
            ],
            khoa_unit_ids,
        )
        if target_user:
            return target_user, target_unit, "SUBMITTED_TO_DEPT_MANAGER"
        return None, None, "Không tìm thấy Kỹ thuật viên trưởng / Phó khoa / Trưởng khoa."

    if _is_ktv_truong(role_codes):
        target_user, target_unit = _pick_first(
            [RoleCode.ROLE_PHO_TRUONG_KHOA, RoleCode.ROLE_TRUONG_KHOA],
            khoa_unit_ids,
        )
        if target_user:
            return target_user, target_unit, "SUBMITTED_TO_DEPT_MANAGER"
        return None, None, "Không tìm thấy Phó khoa / Trưởng khoa."

    if _is_pho_khoa(role_codes):
        target_user, target_unit = _pick_first(
            [RoleCode.ROLE_TRUONG_KHOA, RoleCode.ROLE_BGD, RoleCode.ROLE_LANH_DAO]
        )
        if target_user:
            next_status = "SUBMITTED_TO_HDTV" if _normalize_role_code(next(iter(_load_role_codes_for_user(db, target_user.id)), "")) == "ROLE_LANH_DAO" else "SUBMITTED_TO_DEPT_MANAGER"
            return target_user, target_unit, next_status
        return None, None, "Không tìm thấy Trưởng khoa / BGĐ / HĐTV."

    if _is_truong_khoa(role_codes):
        target_user, target_unit = _pick_first([RoleCode.ROLE_BGD, RoleCode.ROLE_LANH_DAO])
        if target_user:
            target_role_codes = _load_role_codes_for_user(db, target_user.id)
            next_status = "SUBMITTED_TO_HDTV" if _is_board(target_role_codes) else "SUBMITTED_TO_DEPT_MANAGER"
            return target_user, target_unit, next_status
        return None, None, "Không tìm thấy BGĐ / HĐTV."

    if _is_bgd(role_codes):
        target_user, target_unit = _pick_first([RoleCode.ROLE_LANH_DAO])
        if target_user:
            return target_user, target_unit, "SUBMITTED_TO_HDTV"
        return None, None, "Không tìm thấy HĐTV."

    if _is_board(role_codes):
        return user, primary_unit, "SUBMITTED_TO_HDTV"

    return None, None, "Không xác định được tuyến trình phù hợp cho tài khoản này."


def _get_submit_candidates(
    db: Session,
    user: Users,
    role_codes: Set[str],
    primary_unit: Optional[Units],
) -> List[Dict[str, object]]:
    """
    Trả danh sách người nhận hợp lệ để hiển thị dropdown 'Người nhận'.
    Mỗi phần tử gồm:
    - user: đối tượng Users
    - unit: đơn vị chính của người nhận
    - next_status: trạng thái tiếp theo nếu trình cho người này
    """
    if _is_admin(role_codes):
        return []

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else primary_unit

    khoa_unit = None
    if primary_unit:
        if getattr(primary_unit, "cap_do", None) == 2:
            khoa_unit = primary_unit
        elif getattr(primary_unit, "parent_id", None):
            khoa_unit = db.get(Units, primary_unit.parent_id)

    khoa_unit_ids = {khoa_unit.id} if khoa_unit and getattr(khoa_unit, "id", None) else None
    same_team_ids = {primary_unit.id} if primary_unit and getattr(primary_unit, "cap_do", None) == 3 else None

    role_pool: List[RoleCode] = []
    unit_scope: Optional[Set[str]] = khoa_unit_ids
    next_status = "SUBMITTED_TO_DEPT_MANAGER"

    if _is_employee(role_codes):
        role_pool = [RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM, RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO]
        unit_scope = same_team_ids
        next_status = "SUBMITTED_TO_TO_MANAGER"

    elif _is_team_manager(role_codes):
        role_pool = [
            RoleCode.ROLE_QL_CONG_VIEC,
            RoleCode.ROLE_QL_CHAT_LUONG,
            RoleCode.ROLE_QL_KY_THUAT,
            RoleCode.ROLE_QL_AN_TOAN,
            RoleCode.ROLE_QL_VAT_TU,
            RoleCode.ROLE_QL_TRANG_THIET_BI,
            RoleCode.ROLE_QL_MOI_TRUONG,
            RoleCode.ROLE_QL_CNTT,
            RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
        ]
        next_status = "SUBMITTED_TO_TO_MANAGER"

    elif _is_ql_cong_viec(role_codes):
        role_pool = [
            RoleCode.ROLE_QL_CHAT_LUONG,
            RoleCode.ROLE_QL_KY_THUAT,
            RoleCode.ROLE_QL_AN_TOAN,
            RoleCode.ROLE_QL_VAT_TU,
            RoleCode.ROLE_QL_TRANG_THIET_BI,
            RoleCode.ROLE_QL_MOI_TRUONG,
            RoleCode.ROLE_QL_CNTT,
            RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
        ]
        next_status = "SUBMITTED_TO_TO_MANAGER"

    elif _is_functional_manager(role_codes):
        role_pool = [
            RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
            RoleCode.ROLE_PHO_TRUONG_KHOA,
            RoleCode.ROLE_TRUONG_KHOA,
        ]
        next_status = "SUBMITTED_TO_DEPT_MANAGER"

    elif _is_ktv_truong(role_codes):
        role_pool = [RoleCode.ROLE_PHO_TRUONG_KHOA, RoleCode.ROLE_TRUONG_KHOA]
        next_status = "SUBMITTED_TO_DEPT_MANAGER"

    elif _is_pho_khoa(role_codes):
        role_pool = [RoleCode.ROLE_TRUONG_KHOA, RoleCode.ROLE_BGD, RoleCode.ROLE_LANH_DAO]
        unit_scope = None

    elif _is_truong_khoa(role_codes):
        role_pool = [RoleCode.ROLE_BGD, RoleCode.ROLE_LANH_DAO]
        unit_scope = None

    elif _is_bgd(role_codes):
        role_pool = [RoleCode.ROLE_LANH_DAO]
        unit_scope = None
        next_status = "SUBMITTED_TO_HDTV"

    elif _is_board(role_codes):
        return []

    else:
        return []

    rows = _find_users_by_roles(db, role_pool, unit_scope, exclude_user_id=user.id)
    result: List[Dict[str, object]] = []

    for target_user in rows:
        target_membership = _get_primary_membership(db, target_user.id)
        target_unit = db.get(Units, target_membership.unit_id) if target_membership and target_membership.unit_id else None
        target_role_codes = _load_role_codes_for_user(db, target_user.id)

        candidate_status = next_status
        if _is_board(target_role_codes):
            candidate_status = "SUBMITTED_TO_HDTV"
        elif _is_bgd(target_role_codes):
            candidate_status = "SUBMITTED_TO_DEPT_MANAGER"
        elif _is_truong_khoa(target_role_codes) or _is_pho_khoa(target_role_codes) or _is_ktv_truong(target_role_codes):
            if _is_team_manager(role_codes) or _is_ql_cong_viec(role_codes):
                candidate_status = "SUBMITTED_TO_TO_MANAGER"
            else:
                candidate_status = "SUBMITTED_TO_DEPT_MANAGER"

        result.append(
            {
                "user": target_user,
                "unit": target_unit,
                "next_status": candidate_status,
                "display_label": _display_user_option_label(
                    target_user,
                    target_role_codes,
                    target_unit,
                ),
            }
        )

    return result
    
def _get_coordination_candidates(
    db: Session,
    user: Users,
    role_codes: Set[str],
    primary_unit: Optional[Units],
) -> List[Dict[str, object]]:
    """
    Ứng viên phối hợp theo cấu trúc Xét nghiệm, dùng cho dropdown.
    - Nhân viên: nhân viên cùng nhóm/Tổ
    - Nhóm trưởng/Nhóm phó: nhóm trưởng/nhóm phó của các nhóm trong khoa
    - QL công việc: QL công việc cùng cấp trong khoa
    - QL chức năng: QL chức năng cùng cấp trong khoa
    - KTV trưởng: QL công việc + QL chức năng cùng cấp trong khoa
    - Trưởng khoa/Phó khoa: ẩn gửi phối hợp
    - BGĐ: BGĐ
    - HĐTV: HĐTV
    - Admin: không tham gia
    """
    if _is_admin(role_codes):
        return []

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else primary_unit

    khoa_unit = None
    if primary_unit:
        if getattr(primary_unit, "cap_do", None) == 2:
            khoa_unit = primary_unit
        elif getattr(primary_unit, "parent_id", None):
            khoa_unit = db.get(Units, primary_unit.parent_id)

    khoa_unit_ids = {khoa_unit.id} if khoa_unit and getattr(khoa_unit, "id", None) else None
    same_team_ids = {primary_unit.id} if primary_unit and getattr(primary_unit, "cap_do", None) == 3 else None

    role_pool: List[RoleCode] = []
    unit_scope: Optional[Set[str]] = khoa_unit_ids

    if _is_employee(role_codes):
        role_pool = [RoleCode.ROLE_NHAN_VIEN]
        unit_scope = same_team_ids

    elif _is_team_manager(role_codes):
        role_pool = [
            RoleCode.ROLE_TRUONG_NHOM,
            RoleCode.ROLE_PHO_NHOM,
            RoleCode.ROLE_TO_TRUONG,
            RoleCode.ROLE_PHO_TO,
        ]
        unit_scope = khoa_unit_ids

    elif _is_ql_cong_viec(role_codes):
        role_pool = [RoleCode.ROLE_QL_CONG_VIEC]
        unit_scope = khoa_unit_ids

    elif _is_functional_manager(role_codes):
        role_pool = [
            RoleCode.ROLE_QL_CHAT_LUONG,
            RoleCode.ROLE_QL_KY_THUAT,
            RoleCode.ROLE_QL_AN_TOAN,
            RoleCode.ROLE_QL_VAT_TU,
            RoleCode.ROLE_QL_TRANG_THIET_BI,
            RoleCode.ROLE_QL_MOI_TRUONG,
            RoleCode.ROLE_QL_CNTT,
        ]
        unit_scope = khoa_unit_ids

    elif _is_ktv_truong(role_codes):
        role_pool = [
            RoleCode.ROLE_QL_CONG_VIEC,
            RoleCode.ROLE_QL_CHAT_LUONG,
            RoleCode.ROLE_QL_KY_THUAT,
            RoleCode.ROLE_QL_AN_TOAN,
            RoleCode.ROLE_QL_VAT_TU,
            RoleCode.ROLE_QL_TRANG_THIET_BI,
            RoleCode.ROLE_QL_MOI_TRUONG,
            RoleCode.ROLE_QL_CNTT,
        ]
        unit_scope = khoa_unit_ids

    elif _is_pho_khoa(role_codes) or _is_truong_khoa(role_codes):
        return []

    elif _is_bgd(role_codes):
        role_pool = [RoleCode.ROLE_BGD]
        unit_scope = None

    elif _is_board(role_codes):
        role_pool = [RoleCode.ROLE_LANH_DAO]
        unit_scope = None

    else:
        return []

    rows = _find_users_by_roles(db, role_pool, unit_scope, exclude_user_id=user.id)
    result: List[Dict[str, object]] = []

    for target_user in rows:
        target_membership = _get_primary_membership(db, target_user.id)
        target_unit = db.get(Units, target_membership.unit_id) if target_membership and target_membership.unit_id else None
        target_role_codes = _load_role_codes_for_user(db, target_user.id)

        result.append(
            {
                "user": target_user,
                "unit": target_unit,
                "display_label": _display_user_option_label(
                    target_user,
                    target_role_codes,
                    target_unit,
                ),
            }
        )

    return result


def _get_pending_coordination_for_user(db: Session, draft_id: str, user_id: str) -> List[DocumentDraftActions]:
    return (
        db.query(DocumentDraftActions)
        .filter(
            DocumentDraftActions.draft_id == draft_id,
            DocumentDraftActions.action_type == _COORDINATION_ACTION,
            DocumentDraftActions.to_user_id == user_id,
            DocumentDraftActions.is_pending.is_(True),
        )
        .order_by(DocumentDraftActions.created_at.asc())
        .all()
    )


def _can_view_draft(db: Session, draft: DocumentDrafts, user: Users, role_codes: Set[str]) -> bool:
    if _is_admin(role_codes):
        return True

    if _is_board(role_codes):
        return True

    # Người tạo hoặc người đang xử lý hiện tại
    if draft.created_by == user.id or draft.current_handler_user_id == user.id:
        return True

    # Người đang có yêu cầu phối hợp chờ phản hồi
    if _get_pending_coordination_for_user(db, draft.id, user.id):
        return True

    # Người đã thực sự tham gia luồng xử lý
    involved = (
        db.query(DocumentDraftActions.id)
        .filter(
            DocumentDraftActions.draft_id == draft.id,
            (
                (DocumentDraftActions.from_user_id == user.id)
                | (DocumentDraftActions.to_user_id == user.id)
            ),
        )
        .first()
    )
    if involved:
        return True

    return False


def _can_edit_draft(draft: DocumentDrafts, user: Users, role_codes: Set[str]) -> bool:
    if _is_admin(role_codes):
        return False
    return draft.created_by == user.id and draft.current_handler_user_id == user.id and draft.current_status in {"DRAFT", "RETURNED_FOR_EDIT"}


def _can_approve_forward(draft: DocumentDrafts, user: Users, role_codes: Set[str]) -> bool:
    if _is_admin(role_codes):
        return False
    return draft.current_handler_user_id == user.id and draft.current_status in {
        "SUBMITTED_TO_TO_MANAGER",
        "SUBMITTED_TO_DEPT_MANAGER",
        "SUBMITTED_TO_HDTV",
        "IN_COORDINATION",
    }


def _can_finish_draft(draft: DocumentDrafts, user: Users, role_codes: Set[str]) -> bool:
    if _is_admin(role_codes):
        return False

    if draft.current_handler_user_id != user.id:
        return False

    if draft.current_status not in {
        "SUBMITTED_TO_DEPT_MANAGER",
        "SUBMITTED_TO_HDTV",
        "IN_COORDINATION",
    }:
        return False

    return _is_board(role_codes) or _is_truong_khoa(role_codes) or _is_pho_khoa(role_codes)


def _build_draft_row(db: Session, draft: DocumentDrafts) -> Dict[str, object]:
    active_file = _active_file(db, draft.id)
    creator = db.get(Users, draft.created_by) if draft.created_by else None
    handler = db.get(Users, draft.current_handler_user_id) if draft.current_handler_user_id else None
    created_unit = db.get(Units, draft.created_unit_id) if draft.created_unit_id else None
    pending_coord_count = (
        db.query(DocumentDraftActions)
        .filter(
            DocumentDraftActions.draft_id == draft.id,
            DocumentDraftActions.action_type == _COORDINATION_ACTION,
            DocumentDraftActions.is_pending.is_(True),
        )
        .count()
    )
    return {
        "obj": draft,
        "id": draft.id,
        "title": draft.title,
        "document_type": draft.document_type,
        "creator_name": _user_label(creator),
        "created_unit_name": _unit_label(created_unit),
        "handler_name": _user_label(handler),
        "status": draft.current_status,
        "status_label": _status_label(draft.current_status),
        "active_file": active_file,
        "pending_coord_count": pending_coord_count,
    }


def _load_visible_drafts(db: Session, user: Users, role_codes: Set[str], only_mode: str = "") -> List[Dict[str, object]]:
    rows = db.query(DocumentDrafts).filter(DocumentDrafts.is_deleted.is_(False)).order_by(DocumentDrafts.updated_at.desc()).all()
    result: List[Dict[str, object]] = []
    for draft in rows:
        if not _can_view_draft(db, draft, user, role_codes):
            continue
        if only_mode == "mine" and draft.created_by != user.id:
            continue
        if only_mode == "pending" and draft.current_handler_user_id != user.id and not _get_pending_coordination_for_user(db, draft.id, user.id):
            continue
        if only_mode == "finished" and draft.current_status != "FINISHED":
            continue
        if only_mode not in {"", "mine", "pending", "finished"} and draft.current_status != only_mode:
            continue
        result.append(_build_draft_row(db, draft))
    return result


def _view_media_type(file_rec: DocumentDraftFiles) -> str:
    return file_rec.mime_type or mimetypes.guess_type(file_rec.file_name or "")[0] or "application/octet-stream"


def _can_preview_file(file_rec: DocumentDraftFiles) -> bool:
    media_type = _view_media_type(file_rec)
    if media_type in _INLINE_MIME_EXACT:
        return True
    return any(media_type.startswith(prefix) for prefix in _INLINE_MIME_PREFIXES)


def _ensure_draft_access(db: Session, draft_id: str, user: Users, role_codes: Set[str]) -> DocumentDrafts:
    draft = db.get(DocumentDrafts, draft_id)
    if not draft or draft.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy hồ sơ dự thảo.")
    if not _can_view_draft(db, draft, user, role_codes):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập hồ sơ này.")
    return draft


def _load_status_options() -> List[Tuple[str, str]]:
    return [
        ("", "Tất cả"),
        ("mine", "Do tôi tạo"),
        ("pending", "Chờ tôi xử lý"),
        ("finished", "Đã kết thúc"),
        ("DRAFT", "Nháp"),
        ("RETURNED_FOR_EDIT", "Bị trả lại"),
        ("SUBMITTED_TO_TO_MANAGER", "Chờ QL tổ"),
        ("SUBMITTED_TO_DEPT_MANAGER", "Chờ QL phòng"),
        ("SUBMITTED_TO_HDTV", "Chờ HĐTV"),
        ("IN_COORDINATION", "Đang phối hợp"),
        ("FINISHED", "Kết thúc"),
    ]


@router.get("", response_class=HTMLResponse)
def draft_approval_index(
    request: Request,
    selected_id: str = "",
    status: str = "",
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    drafts = _load_visible_drafts(db, user, role_codes, status)
    selected_draft = None
    if selected_id:
        selected_draft = _ensure_draft_access(db, selected_id, user, role_codes)
    elif drafts:
        selected_draft = drafts[0]["obj"]

    detail = None
    if selected_draft:
        active_file = _active_file(db, selected_draft.id)
        files = (
            db.query(DocumentDraftFiles)
            .filter(DocumentDraftFiles.draft_id == selected_draft.id, DocumentDraftFiles.is_deleted.is_(False))
            .order_by(DocumentDraftFiles.uploaded_at.desc())
            .all()
        )
        actions = (
            db.query(DocumentDraftActions)
            .filter(DocumentDraftActions.draft_id == selected_draft.id)
            .order_by(DocumentDraftActions.created_at.asc())
            .all()
        )
        pending_coord = _get_pending_coordination_for_user(db, selected_draft.id, user.id)
        coord_candidates = _get_coordination_candidates(db, user, role_codes, primary_unit) if selected_draft.current_handler_user_id == user.id else []
        submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit) if selected_draft.current_handler_user_id == user.id else []

        for file_row in files:
            setattr(file_row, "file_role_label", _file_role_label(getattr(file_row, "file_role", None)))
            setattr(file_row, "can_preview", _can_preview_file(file_row))

        if active_file:
            setattr(active_file, "can_preview", _can_preview_file(active_file))

        for action_row in actions:
            setattr(action_row, "action_label", _action_label(getattr(action_row, "action_type", None)))

        detail = {
            "draft": selected_draft,
            "active_file": active_file,
            "files": files,
            "actions": actions,
            "status_label": _status_label(selected_draft.current_status),
            "pending_coord": pending_coord,
            "coord_candidates": [] if _is_admin(role_codes) else coord_candidates,
            "submit_candidates": [] if _is_admin(role_codes) else submit_candidates,
            "can_edit": _can_edit_draft(selected_draft, user, role_codes),
            "can_approve_forward": _can_approve_forward(selected_draft, user, role_codes),
            "can_finish_draft": _can_finish_draft(selected_draft, user, role_codes),
            "is_hdtv_handler": selected_draft.current_handler_user_id == user.id and _is_board(role_codes),
            "is_lab_lead_handler": selected_draft.current_handler_user_id == user.id and (_is_truong_khoa(role_codes) or _is_pho_khoa(role_codes)),
        }

    return templates.TemplateResponse(
        "draft_approval.html",
        {
            "request": request,
            "app_name": getattr(settings, "APP_NAME", "HVGL_Workspace"),
            "company_name": getattr(settings, "COMPANY_NAME", ""),
            "draft_rows": drafts,
            "selected_detail": detail,
            "status_options": _load_status_options(),
            "selected_status": status,
            "selected_id": selected_draft.id if selected_draft else "",
            "me": user,
            "me_role_codes": role_codes,
            "primary_unit": primary_unit,
        },
    )


@router.post("/create")
async def create_draft(
    request: Request,
    title: str = Form(...),
    document_type: str = Form("Dự thảo văn bản"),
    summary: str = Form(""),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    primary_membership = _get_primary_membership(db, user.id)
    if not primary_membership or not primary_membership.unit_id:
        return RedirectResponse(url="/draft-approvals?error=Tài khoản chưa được gán đơn vị chính.", status_code=302)

    primary_unit = db.get(Units, primary_membership.unit_id)
    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url="/draft-approvals?error=Chưa chọn đúng định dạng tài liệu.", status_code=302)

    draft = DocumentDrafts(
        title=(title or "").strip(),
        document_type=(document_type or "Dự thảo văn bản").strip(),
        summary=(summary or "").strip(),
        created_by=user.id,
        created_unit_id=primary_unit.id if primary_unit else None,
        current_status="DRAFT",
        current_handler_user_id=user.id,
        current_handler_unit_id=primary_unit.id if primary_unit else None,
        current_role_code="CREATOR",
        last_submitter_id=user.id,
    )
    db.add(draft)
    db.flush()
    file_rec = _add_file_record(db, draft, upfile, user.id, "DRAFT_UPLOAD", activate=True)
    _log_action(
        db,
        draft,
        action_type="CREATE",
        from_user_id=user.id,
        to_user_id=user.id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=primary_unit.id if primary_unit else None,
        comment="Tạo hồ sơ dự thảo.",
        linked_file_id=file_rec.id,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_created",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(user.id),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users([str(user.id)], payload)
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Tạo hồ sơ dự thảo thành công.", status_code=302)


@router.post("/{draft_id}/upload")
async def upload_replacement_file(
    draft_id: str,
    request: Request,
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if not _can_edit_draft(draft, user):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền upload thay thế ở bước hiện tại.", status_code=302)
    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Định dạng tài liệu không được hỗ trợ.", status_code=302)

    rec = _add_file_record(db, draft, upfile, user.id, "DRAFT_UPLOAD", activate=True)
    draft.updated_at = _now()
    _log_action(db, draft, "UPLOAD_REPLACEMENT", from_user_id=user.id, to_user_id=user.id, comment="Cập nhật tài liệu dự thảo.", linked_file_id=rec.id)
    db.commit()
    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Upload tài liệu thành công.", status_code=302)


@router.post("/{draft_id}/submit")
async def submit_draft(
    draft_id: str,
    request: Request,
    recipient_id: str = Form(""),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Hồ sơ này không nằm ở bước xử lý của bạn.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit)
    candidate_map = {str(item["user"].id): item for item in submit_candidates if item.get("user")}

    if not recipient_id or recipient_id not in candidate_map:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Phải chọn đúng người nhận hợp lệ trước khi trình.",
            status_code=302,
        )

    selected_candidate = candidate_map[recipient_id]
    target_user = selected_candidate["user"]
    target_unit = selected_candidate["unit"]
    next_status = str(selected_candidate["next_status"] or "").strip()

    if not target_user or not next_status:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Không xác định được người nhận hoặc trạng thái trình tiếp theo.",
            status_code=302,
        )

    draft.current_status = next_status
    draft.current_handler_user_id = target_user.id
    draft.current_handler_unit_id = target_unit.id if target_unit else None
    draft.current_role_code = ",".join(sorted(_load_role_codes_for_user(db, target_user.id)))
    draft.last_submitter_id = user.id
    draft.last_submitted_at = _now()
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="SUBMIT",
        from_user_id=user.id,
        to_user_id=target_user.id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=target_unit.id if target_unit else None,
        comment=(comment or "").strip() or "Trình dự thảo lên cấp phê duyệt tiếp theo.",
        linked_file_id=_active_file(db, draft.id).id if _active_file(db, draft.id) else None,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_submitted",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(target_user.id),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(target_user.id), str(user.id)],
            payload,
        )
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Trình dự thảo thành công.", status_code=302)


@router.post("/{draft_id}/approve")
async def approve_forward(
    draft_id: str,
    request: Request,
    recipient_id: str = Form(""),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)

    if _is_admin(role_codes):
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Admin chỉ được xem hồ sơ, không tham gia trình và duyệt văn bản.",
            status_code=302,
        )

    if not _can_approve_forward(draft, user, role_codes):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền phê duyệt hồ sơ này.", status_code=302)

    active_file = _active_file(db, draft.id)
    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    if _is_board(role_codes):
        draft.current_status = "FINISHED"
        draft.current_handler_user_id = draft.last_submitter_id
        draft.current_handler_unit_id = draft.created_unit_id
        draft.current_role_code = "FINISHED"
        draft.finished_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="HDTV_APPROVED",
            from_user_id=user.id,
            to_user_id=draft.last_submitter_id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=draft.current_handler_unit_id,
            comment=(comment or "").strip() or "HĐTV phê duyệt nội dung văn bản. Hồ sơ tại tab này kết thúc.",
            linked_file_id=active_file.id if active_file else None,
        )
    else:
        submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit)
        candidate_map = {str(item["user"].id): item for item in submit_candidates if item.get("user")}

        if not recipient_id or recipient_id not in candidate_map:
            return RedirectResponse(
                url=f"/draft-approvals?selected_id={draft.id}&error=Phải chọn đúng người nhận hợp lệ trước khi trình tiếp.",
                status_code=302,
            )

        selected_candidate = candidate_map[recipient_id]
        target_user = selected_candidate["user"]
        target_unit = selected_candidate["unit"]
        next_status = str(selected_candidate["next_status"] or "").strip()

        if not target_user or not next_status:
            return RedirectResponse(
                url=f"/draft-approvals?selected_id={draft.id}&error=Không xác định được cấp phê duyệt tiếp theo.",
                status_code=302,
            )

        draft.current_status = next_status
        draft.current_handler_user_id = target_user.id
        draft.current_handler_unit_id = target_unit.id if target_unit else None
        draft.current_role_code = ",".join(sorted(_load_role_codes_for_user(db, target_user.id)))
        draft.last_submitter_id = user.id
        draft.last_submitted_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="APPROVE_FORWARD",
            from_user_id=user.id,
            to_user_id=target_user.id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=target_unit.id if target_unit else None,
            comment=(comment or "").strip() or "Đồng ý nội dung và trình cấp trên tiếp theo.",
            linked_file_id=active_file.id if active_file else None,
        )
    db.commit()

    try:
        if _is_board(role_codes):
            payload = {
                "module": "draft",
                "type": "draft_approved",
                "draft_id": str(draft.id),
                "from_user_id": str(user.id),
                "to_user_id": str(draft.last_submitter_id or ""),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_draft_users(
                [str(draft.last_submitter_id or ""), str(user.id)],
                payload,
            )
        else:
            payload = {
                "module": "draft",
                "type": "draft_submitted",
                "draft_id": str(draft.id),
                "from_user_id": str(user.id),
                "to_user_id": str(target_user.id),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_draft_users(
                [str(target_user.id), str(user.id)],
                payload,
            )
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Xử lý phê duyệt thành công.", status_code=302)

@router.post("/{draft_id}/finish")
async def finish_draft(
    draft_id: str,
    request: Request,
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)

    if not _can_finish_draft(draft, user, role_codes):
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền phê duyệt hoàn thành hồ sơ này.",
            status_code=302,
        )

    active_file = _active_file(db, draft.id)
    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    return_user_id = draft.last_submitter_id or draft.created_by
    return_membership = _get_primary_membership(db, return_user_id) if return_user_id else None

    draft.current_status = "FINISHED"
    draft.current_handler_user_id = return_user_id
    draft.current_handler_unit_id = return_membership.unit_id if return_membership else draft.created_unit_id
    draft.current_role_code = "FINISHED"
    draft.finished_at = _now()
    draft.updated_at = _now()

    finish_comment = (comment or "").strip()
    if not finish_comment:
        if _is_board(role_codes):
            finish_comment = "HĐTV phê duyệt hoàn thành văn bản dự thảo. Hồ sơ tại tab này kết thúc."
        elif _is_truong_khoa(role_codes):
            finish_comment = "Trưởng khoa phê duyệt hoàn thành văn bản dự thảo. Hồ sơ tại tab này kết thúc."
        else:
            finish_comment = "Phó khoa phê duyệt hoàn thành văn bản dự thảo. Hồ sơ tại tab này kết thúc."

    _log_action(
        db,
        draft,
        action_type="FINISHED",
        from_user_id=user.id,
        to_user_id=return_user_id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.current_handler_unit_id,
        comment=finish_comment,
        linked_file_id=active_file.id if active_file else None,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_approved",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(return_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(return_user_id or ""), str(user.id)],
            payload,
        )
    except Exception:
        pass

    return RedirectResponse(
        url=f"/draft-approvals?selected_id={draft.id}&msg=Đã phê duyệt hoàn thành hồ sơ dự thảo.",
        status_code=302,
    )
    
    
@router.post("/{draft_id}/return")
async def return_for_edit(
    draft_id: str,
    request: Request,
    comment: str = Form(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không phải người đang xử lý hồ sơ.", status_code=302)
    if not (comment or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải nhập ý kiến sửa đổi, bổ sung khi trả lại.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None
    return_user_id = draft.last_submitter_id or draft.created_by
    return_membership = _get_primary_membership(db, return_user_id) if return_user_id else None
    draft.current_status = "RETURNED_FOR_EDIT"
    draft.current_handler_user_id = return_user_id
    draft.current_handler_unit_id = return_membership.unit_id if return_membership else draft.created_unit_id
    draft.current_role_code = "RETURNED"
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="RETURN_FOR_EDIT",
        from_user_id=user.id,
        to_user_id=return_user_id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.current_handler_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=_active_file(db, draft.id).id if _active_file(db, draft.id) else None,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_returned",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(return_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(return_user_id or ""), str(user.id)],
            payload,
        )
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Đã trả lại hồ sơ để chỉnh sửa.", status_code=302)


@router.post("/{draft_id}/return-edited")
async def return_with_edited_file(
    draft_id: str,
    request: Request,
    comment: str = Form(...),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không phải người đang xử lý hồ sơ.", status_code=302)
    if not (comment or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải nhập ý kiến sửa đổi, bổ sung khi trả lại.", status_code=302)
    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải upload file đã sửa hợp lệ khi trả lại theo luồng này.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None
    return_user_id = draft.last_submitter_id or draft.created_by
    return_membership = _get_primary_membership(db, return_user_id) if return_user_id else None
    returned_file = _add_file_record(db, draft, upfile, user.id, "RETURNED_EDITED_FILE", activate=True)
    draft.current_status = "RETURNED_FOR_EDIT"
    draft.current_handler_user_id = return_user_id
    draft.current_handler_unit_id = return_membership.unit_id if return_membership else draft.created_unit_id
    draft.current_role_code = "RETURNED"
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="RETURN_WITH_EDITED_FILE",
        from_user_id=user.id,
        to_user_id=return_user_id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.current_handler_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=returned_file.id,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_returned",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(return_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(return_user_id or ""), str(user.id)],
            payload,
        )
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Đã trả lại hồ sơ kèm file đã sửa.", status_code=302)


@router.post("/{draft_id}/coordinate")
async def send_for_coordination(
    draft_id: str,
    request: Request,
    recipient_ids: List[str] = Form([]),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)

    if draft.current_handler_user_id != user.id:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Chỉ người đang xử lý chính mới được gửi phối hợp.",
            status_code=302,
        )

    clean_recipient_ids: List[str] = []
    for rid in recipient_ids or []:
        rid = str(rid or "").strip()
        if rid and rid not in clean_recipient_ids:
            clean_recipient_ids.append(rid)

    if not clean_recipient_ids:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Chưa chọn người nhận phối hợp.",
            status_code=302,
        )

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    coord_candidates = _get_coordination_candidates(db, user, role_codes, primary_unit)
    candidate_map = {str(item["user"].id): item for item in coord_candidates if item.get("user")}

    invalid_ids = [rid for rid in clean_recipient_ids if rid not in candidate_map]
    if invalid_ids:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Có người nhận phối hợp không hợp lệ.",
            status_code=302,
        )

    draft.current_status = "IN_COORDINATION"
    draft.updated_at = _now()
    linked_file = _active_file(db, draft.id)

    notify_user_ids: List[str] = [str(user.id)]

    for recipient_id in clean_recipient_ids:
        rec_m = _get_primary_membership(db, recipient_id)
        _log_action(
            db,
            draft,
            action_type=_COORDINATION_ACTION,
            from_user_id=user.id,
            to_user_id=recipient_id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=rec_m.unit_id if rec_m else None,
            comment=(comment or "").strip() or "Đề nghị phối hợp góp ý dự thảo văn bản.",
            linked_file_id=linked_file.id if linked_file else None,
            is_pending=True,
        )
        if recipient_id not in notify_user_ids:
            notify_user_ids.append(recipient_id)

    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_coordination_requested",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_ids": clean_recipient_ids,
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(notify_user_ids, payload)
    except Exception:
        pass

    return RedirectResponse(
        url=f"/draft-approvals?selected_id={draft.id}&msg=Đã gửi phối hợp thành công cho {len(clean_recipient_ids)} người.",
        status_code=302,
    )


@router.post("/{draft_id}/coordinate-reply/{action_id}")
async def reply_coordination(
    draft_id: str,
    action_id: str,
    request: Request,
    response_text: str = Form(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ = _ensure_draft_access(db, draft_id, user, role_codes)
    action = db.get(DocumentDraftActions, action_id)
    if not action or action.draft_id != draft_id or action.action_type != _COORDINATION_ACTION:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Không tìm thấy yêu cầu phối hợp.", status_code=302)
    if action.to_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Bạn không phải người nhận phối hợp của yêu cầu này.", status_code=302)
    if not (response_text or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Phải nhập ý kiến phản hồi phối hợp.", status_code=302)

    action.response_text = (response_text or "").strip()
    action.responded_at = _now()
    action.is_pending = False
    db.add(action)
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_coordination_replied",
            "draft_id": str(draft_id),
            "from_user_id": str(user.id),
            "to_user_id": str(action.from_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(action.from_user_id or ""), str(user.id)],
            payload,
        )
    except Exception:
        pass

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&msg=Đã phản hồi phối hợp.", status_code=302)


@router.get("/file/{file_id}/download")
def draft_file_download(file_id: str, request: Request, db: Session = Depends(get_db)):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    rec = db.get(DocumentDraftFiles, file_id)
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu.")
    draft = _ensure_draft_access(db, rec.draft_id, user, role_codes)
    _ = draft
    if not rec.file_path or not os.path.exists(rec.file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")
    return FileResponse(rec.file_path, filename=rec.file_name, media_type=_view_media_type(rec))


@router.get("/file/{file_id}/view")
def draft_file_view(file_id: str, request: Request, db: Session = Depends(get_db)):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    rec = db.get(DocumentDraftFiles, file_id)
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu.")
    _ensure_draft_access(db, rec.draft_id, user, role_codes)
    if not rec.file_path or not os.path.exists(rec.file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    media_type = _view_media_type(rec)

    # Không tự dựng Content-Disposition với tên file tiếng Việt,
    # tránh lỗi UnicodeEncodeError của Starlette khi encode header theo latin-1.
    # Trình duyệt vẫn inline được với PDF/ảnh/text dựa trên media_type.
    return FileResponse(rec.file_path, media_type=media_type)
