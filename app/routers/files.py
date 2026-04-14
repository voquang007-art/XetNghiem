# -*- coding: utf-8 -*-
"""
app/routers/files.py

Tab "Tài liệu" = Kho tài liệu sử dụng chung cho đơn vị.

"""

import hashlib
import mimetypes
import os
import pathlib
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Set, List

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from sqlalchemy import func
from sqlalchemy.orm import Session
from starlette.responses import FileResponse, RedirectResponse
from starlette.templating import Jinja2Templates

from ..config import settings
from ..models import (
    Files, UserUnitMemberships, Units, Roles, UserRoles, RoleCode,
    VisibilityGrants, ManagementScopes, ScopePermissions, PermissionCode, UnitStatus
)
from ..security.deps import get_db, login_required

router = APIRouter()

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

# =========================
# Cấu hình loại file cho phép
# =========================
ALLOWED_EXTENSIONS = {
    ".doc", ".docx",
    ".xls", ".xlsx",
    ".pdf",
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv",
}

DOCUMENT_EXTENSIONS = {".doc", ".docx"}
SPREADSHEET_EXTENSIONS = {".xls", ".xlsx"}
PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv"}

SORT_FIELDS = {
    "name": "name",
    "type": "type",
    "owner": "owner",
    "uploaded_at": "uploaded_at",
    "size": "size",
}

ROLE_ADMIN = "ROLE_ADMIN"
ROLE_LANH_DAO = "ROLE_LANH_DAO"
ROLE_BGD = "ROLE_BGD"
ROLE_TRUONG_PHONG = "ROLE_TRUONG_PHONG"
ROLE_PHO_PHONG = "ROLE_PHO_PHONG"
ROLE_TRUONG_KHOA = "ROLE_TRUONG_KHOA"
ROLE_PHO_TRUONG_KHOA = "ROLE_PHO_TRUONG_KHOA"
ROLE_KY_THUAT_VIEN_TRUONG = "ROLE_KY_THUAT_VIEN_TRUONG"
ROLE_TO_TRUONG = "ROLE_TO_TRUONG"
ROLE_PHO_TO = "ROLE_PHO_TO"
ROLE_TRUONG_NHOM = "ROLE_TRUONG_NHOM"
ROLE_PHO_NHOM = "ROLE_PHO_NHOM"
ROLE_QL_CHAT_LUONG = "ROLE_QL_CHAT_LUONG"
ROLE_QL_KY_THUAT = "ROLE_QL_KY_THUAT"
ROLE_QL_AN_TOAN = "ROLE_QL_AN_TOAN"
ROLE_QL_VAT_TU = "ROLE_QL_VAT_TU"
ROLE_QL_TRANG_THIET_BI = "ROLE_QL_TRANG_THIET_BI"
ROLE_QL_MOI_TRUONG = "ROLE_QL_MOI_TRUONG"
ROLE_QL_CNTT = "ROLE_QL_CNTT"


# =========================
# Helpers
# =========================
def _ensure_dir(dir_path: str) -> None:
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def _get_upload_dir() -> str:
    return getattr(settings, "UPLOAD_DIR", os.path.join("instance", "uploads"))


def _get_max_file_bytes() -> int:
    max_mb = getattr(settings, "MAX_FILE_SIZE_MB", 25)
    try:
        max_mb = int(max_mb)
    except Exception:
        max_mb = 25
    return max_mb * 1024 * 1024


def _get_file_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower().strip()


def _is_allowed_extension(filename: str) -> bool:
    return _get_file_ext(filename) in ALLOWED_EXTENSIONS


def _get_file_kind(filename: str) -> str:
    ext = _get_file_ext(filename)
    if ext in DOCUMENT_EXTENSIONS:
        return "document"
    if ext in SPREADSHEET_EXTENSIONS:
        return "spreadsheet"
    if ext in PDF_EXTENSIONS:
        return "pdf"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "other"


def _get_file_kind_label(filename: str) -> str:
    kind = _get_file_kind(filename)
    mapping = {
        "document": "Word",
        "spreadsheet": "Excel",
        "pdf": "PDF",
        "image": "Ảnh",
        "video": "Video",
        "other": "Khác",
    }
    return mapping.get(kind, "Khác")


def _can_inline_preview(filename: str) -> bool:
    return _get_file_kind(filename) in {"pdf", "image", "video"}


def _guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"


def _format_size(size_bytes: Optional[int]) -> str:
    size = int(size_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    value = float(size)
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.1f} {units[idx]}"


def _safe_unique_path(local_dir: str, original_name: str) -> str:
    """
    Tránh ghi đè file trùng tên.
    """
    _ensure_dir(local_dir)
    base_name, ext = os.path.splitext(original_name or "file")
    candidate = os.path.join(local_dir, original_name)

    if not os.path.exists(candidate):
        return candidate

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    counter = 1
    while True:
        new_name = f"{base_name}_{stamp}_{counter}{ext}"
        candidate = os.path.join(local_dir, new_name)
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _save_upload(local_dir: str, up: UploadFile) -> Tuple[str, int, str]:
    """
    Ghi file theo stream an toàn.
    Trả về: (đường_dẫn_tuyệt_đối, kích_thước_bytes, sha256_hex)
    """
    _ensure_dir(local_dir)
    dest = _safe_unique_path(local_dir, up.filename)
    size = 0
    h = hashlib.sha256()
    with open(dest, "wb") as f:
        while True:
            chunk = up.file.read(1024 * 1024)  # 1MB/chunk
            if not chunk:
                break
            size += len(chunk)
            f.write(chunk)
            h.update(chunk)
    up.file.close()
    return dest, size, h.hexdigest()


def _get_primary_unit_id(db: Session, user_id: str) -> Optional[str]:
    """
    Xác định đơn vị chính dùng cho Tab Tài liệu.

    Quy tắc:
    1) Nếu có membership is_primary=True => dùng đơn vị đó.
    2) Nếu không có is_primary:
       - Ưu tiên đơn vị sâu nhất trong cây tổ chức (cap_do lớn nhất),
         tức Tổ (cấp 3) ưu tiên hơn Phòng (cấp 2).
       - Nếu cùng cấp thì lấy bản ghi đầu tiên ổn định theo unit_id.
    """
    membership = (
        db.query(UserUnitMemberships)
        .filter(
            UserUnitMemberships.user_id == user_id,
            UserUnitMemberships.is_primary == True,  # noqa: E712
        )
        .first()
    )
    if membership and membership.unit_id:
        return membership.unit_id

    rows = (
        db.query(UserUnitMemberships, Units)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(Units.cap_do.desc(), Units.id.asc())
        .all()
    )

    if rows:
        membership_obj, _unit_obj = rows[0]
        return membership_obj.unit_id

    return None


def _get_unit(db: Session, unit_id: Optional[str]) -> Optional[Units]:
    if not unit_id:
        return None
    return db.get(Units, unit_id)


def _get_direct_child_unit_ids(db: Session, unit_id: Optional[str]) -> List[str]:
    if not unit_id:
        return []
    rows = (
        db.query(Units.id)
        .filter(Units.parent_id == unit_id)
        .all()
    )
    return [r[0] for r in rows]


def _user_primary_units(db: Session, user_id: str) -> List[Units]:
    mems = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )
    prims = [m for m in mems if getattr(m, "is_primary", False)]
    ids = [m.unit_id for m in (prims or mems) if getattr(m, "unit_id", None)]
    if not ids:
        return []
    return db.query(Units).filter(Units.id.in_(ids)).all()


def _user_membership_units(db: Session, user_id: str) -> List[Units]:
    """
    Lấy toàn bộ đơn vị mà user đang thuộc (không phụ thuộc is_primary).
    Chỉ lấy đơn vị cấp Phòng (2) và Tổ (3).
    """
    rows = (
        db.query(Units)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .filter(Units.cap_do.in_([2, 3]))
        .all()
    )

    dedup = {}
    for u in rows:
        if u and getattr(u, "id", None):
            dedup[u.id] = u

    return list(dedup.values())
    
    
def _get_uploadable_units(db: Session, user_id: str, role_codes: Set[str]) -> List[Units]:
    """
    Cấu trúc Xét nghiệm:
    - HĐTV, BGĐ: không dùng tab Tài liệu.
    - Admin: được chọn toàn bộ đơn vị ACTIVE cấp 2/3 trong phạm vi app.
    - Trưởng khoa / Phó khoa / KTV trưởng: được chọn Khoa và các Nhóm/Tổ trực thuộc.
    - Nhóm/Tổ trưởng, Nhóm/Tổ phó: chỉ được chọn đúng Nhóm/Tổ mình phụ trách.
    - Các vị trí khác: theo membership hiện có.
    """
    if _is_files_tab_hidden(role_codes):
        return []

    if _is_system_admin(role_codes):
        allowed_ids = {
            row[0]
            for row in db.query(Units.id)
            .filter(Units.trang_thai == UnitStatus.ACTIVE, Units.cap_do.in_([2, 3]))
            .all()
        }
    elif _is_lab_lead(role_codes) or _is_ktv_truong(role_codes):
        allowed_ids = {
            row[0]
            for row in db.query(Units.id)
            .filter(Units.trang_thai == UnitStatus.ACTIVE, Units.cap_do.in_([2, 3]))
            .all()
        }
    elif _is_group_lead(role_codes):
        allowed_ids = _group_lead_unit_ids(db, user_id)
    else:
        allowed_ids = {u.id for u in _user_membership_units(db, user_id)}

    units = db.query(Units).filter(Units.id.in_(list(allowed_ids))).all() if allowed_ids else []
    dedup = {}
    for u in units:
        if u and getattr(u, "id", None):
            dedup[u.id] = u

    return sorted(
        dedup.values(),
        key=lambda u: (
            getattr(u, "cap_do", 999) if getattr(u, "cap_do", None) is not None else 999,
            getattr(u, "order_index", 0) or 0,
            getattr(u, "ten_don_vi", "") or "",
        )
    )


def _can_upload_to_unit(db: Session, user_id: str, role_codes: Set[str], target_unit_id: Optional[str]) -> bool:
    if not target_unit_id:
        return False
    allowed_ids = {u.id for u in _get_uploadable_units(db, user_id, role_codes)}
    return target_unit_id in allowed_ids


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    codes: Set[str] = set()
    for (c,) in rows:
        code_up = str(getattr(c, "value", c)).upper() if c is not None else ""
        if code_up:
            codes.add(code_up)
    return codes


def _user_membership_unit_ids(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]


def _active_visibility_modes(db: Session, user_id: str) -> Set[str]:
    unit_ids = _user_membership_unit_ids(db, user_id)
    if not unit_ids:
        return set()

    now = datetime.utcnow()
    grants = (
        db.query(VisibilityGrants)
        .filter(VisibilityGrants.grantee_unit_id.in_(unit_ids))
        .all()
    )

    modes: Set[str] = set()
    for g in grants:
        if g.effective_from and g.effective_from > now:
            continue
        if g.effective_to and g.effective_to < now:
            continue
        mode_val = getattr(g.mode, "value", g.mode)
        if mode_val:
            modes.add(str(mode_val).upper())
    return modes


def _has_files_visibility_grant(db: Session, user_id: str) -> bool:
    modes = _active_visibility_modes(db, user_id)
    return ("VIEW_ALL" in modes) or ("FILES_ONLY" in modes)

def _is_system_admin(role_codes: Set[str]) -> bool:
    return ROLE_ADMIN in role_codes


def _is_hdtv(role_codes: Set[str]) -> bool:
    return ROLE_LANH_DAO in role_codes


def _is_bgd(role_codes: Set[str]) -> bool:
    return ROLE_BGD in role_codes


def _is_lab_lead(role_codes: Set[str]) -> bool:
    return bool({ROLE_TRUONG_KHOA, ROLE_PHO_TRUONG_KHOA} & role_codes)

def _is_ktv_truong(role_codes: Set[str]) -> bool:
    return ROLE_KY_THUAT_VIEN_TRUONG in role_codes

def _is_group_lead(role_codes: Set[str]) -> bool:
    return bool({ROLE_TO_TRUONG, ROLE_PHO_TO, ROLE_TRUONG_NHOM, ROLE_PHO_NHOM} & role_codes)

def _group_lead_unit_ids(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id, Units.cap_do)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )
    team_ids = {unit_id for unit_id, cap_do in rows if unit_id and cap_do == 3}
    if team_ids:
        return team_ids
    return {unit_id for unit_id, _cap_do in rows if unit_id}

def _is_functional_manager(role_codes: Set[str]) -> bool:
    return bool({ROLE_QL_CHAT_LUONG, ROLE_QL_KY_THUAT, ROLE_QL_AN_TOAN} & role_codes)

def _is_operations_manager(role_codes: Set[str]) -> bool:
    return bool({ROLE_QL_VAT_TU, ROLE_QL_TRANG_THIET_BI, ROLE_QL_MOI_TRUONG, ROLE_QL_CNTT} & role_codes)

def _is_matrix_manager(role_codes: Set[str]) -> bool:
    return _is_functional_manager(role_codes) or _is_operations_manager(role_codes)

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
    now = datetime.utcnow()
    query = db.query(ManagementScopes).filter(ManagementScopes.manager_user_id == user_id, ManagementScopes.is_active.is_(True))
    rows = query.all()
    scope_ids = []
    unit_ids: Set[str] = set()
    for row in rows:
        if getattr(row, 'effective_from', None) and row.effective_from > now:
            continue
        if getattr(row, 'effective_to', None) and row.effective_to < now:
            continue
        if getattr(row, 'target_unit_id', None):
            scope_ids.append(row.id)
            unit_ids.add(str(row.target_unit_id))
    if permission_code and scope_ids:
        allowed_scope_ids = {sid for (sid,) in db.query(ScopePermissions.scope_id).filter(ScopePermissions.scope_id.in_(scope_ids), ScopePermissions.permission_code == permission_code).all()}
        unit_ids = {str(row.target_unit_id) for row in rows if row.id in allowed_scope_ids and getattr(row, 'target_unit_id', None)}
    return _descendant_unit_ids(db, unit_ids)

def _matrix_unit_ids_for_files(db: Session, user_id: str, role_codes: Set[str], permission_code: Optional[str] = None) -> Set[str]:
    ids = {u.id for u in _user_membership_units(db, user_id)}
    if _is_bgd_or_lab_lead(role_codes) or _is_ktv_truong(role_codes):
        ids |= {row[0] for row in db.query(Units.id).filter(Units.trang_thai == UnitStatus.ACTIVE).all()} if hasattr(Units, 'trang_thai') else {row[0] for row in db.query(Units.id).all()}
    elif _is_matrix_manager(role_codes):
        ids |= _managed_scope_unit_ids(db, user_id, permission_code)
    return ids

def _is_admin_or_leader(role_codes: Set[str]) -> bool:
    return _is_system_admin(role_codes) or _is_hdtv(role_codes)


def _is_room_manager(role_codes: Set[str]) -> bool:
    return ROLE_TRUONG_KHOA in role_codes or ROLE_PHO_TRUONG_KHOA in role_codes or ROLE_KY_THUAT_VIEN_TRUONG in role_codes

def _is_files_tab_hidden(role_codes: Set[str]) -> bool:
    return _is_hdtv(role_codes) or _is_bgd(role_codes)
    
    
def _can_user_view_file_by_membership(
    *,
    db: Session,
    user_id: str,
    role_codes: Set[str],
    file_unit_id: Optional[str],
) -> bool:
    """
    Cấu trúc Xét nghiệm:
    - HĐTV, BGĐ: không thấy Tab/file.
    - Admin: thấy toàn bộ file.
    - File phạm vi Khoa (cap_do=2): mọi member thuộc Khoa đều thấy.
    - File phạm vi Nhóm/Tổ (cap_do=3): chỉ member thuộc đúng Nhóm/Tổ đó thấy;
      riêng Trưởng khoa, Phó khoa, KTV trưởng được thấy.
    """
    if not file_unit_id:
        return False

    if _is_files_tab_hidden(role_codes):
        return False

    if _is_system_admin(role_codes):
        return True

    file_unit = _get_unit(db, file_unit_id)
    if not file_unit:
        return False

    member_units = _user_membership_units(db, user_id)
    if not member_units:
        return False

    room_ids = set()
    team_ids = set()

    for u in member_units:
        if not u:
            continue
        if getattr(u, "cap_do", None) == 2:
            room_ids.add(u.id)
        elif getattr(u, "cap_do", None) == 3:
            team_ids.add(u.id)
            if getattr(u, "parent_id", None):
                room_ids.add(u.parent_id)

    # File phạm vi Khoa: tất cả member thuộc Khoa đều thấy
    if getattr(file_unit, "cap_do", None) == 2:
        return file_unit.id in room_ids

    # File phạm vi Nhóm/Tổ
    if getattr(file_unit, "cap_do", None) == 3:
        if file_unit.id in team_ids:
            return True
        if file_unit.parent_id in room_ids and (_is_lab_lead(role_codes) or _is_ktv_truong(role_codes)):
            return True
        return False

    member_unit_ids = {u.id for u in member_units if u and getattr(u, "id", None)}
    return file_unit_id in member_unit_ids

def _can_delete_file_by_membership(
    *,
    db: Session,
    user_id: str,
    role_codes: Set[str],
    file_unit_id: Optional[str],
    owner_id: Optional[str] = None,
) -> bool:
    """
    Quyền xóa theo cấu trúc Xét nghiệm:
    - Admin: được xóa.
    - HĐTV, BGĐ: không dùng tab này, không xóa.
    - Người tải file lên: được xóa.
    - Trưởng khoa, Phó khoa, KTV trưởng: được xóa file trong phạm vi Khoa/nhóm trực thuộc.
    - Nhóm trưởng, Nhóm phó: chỉ được xóa file thuộc đúng Nhóm/Tổ của mình.
    """
    if not file_unit_id:
        return False

    if _is_files_tab_hidden(role_codes):
        return False

    if _is_system_admin(role_codes):
        return True

    if owner_id and str(owner_id) == str(user_id):
        return True

    file_unit = _get_unit(db, file_unit_id)
    if not file_unit:
        return False

    member_units = _user_membership_units(db, user_id)
    if not member_units:
        return False

    room_ids = set()
    team_ids = set()

    for u in member_units:
        if not u:
            continue
        if getattr(u, "cap_do", None) == 2:
            room_ids.add(u.id)
        elif getattr(u, "cap_do", None) == 3:
            team_ids.add(u.id)
            if getattr(u, "parent_id", None):
                room_ids.add(u.parent_id)

    if (_is_lab_lead(role_codes) or _is_ktv_truong(role_codes)) and (
        file_unit.id in room_ids or getattr(file_unit, "parent_id", None) in room_ids
    ):
        return True

    if _is_group_lead(role_codes) and getattr(file_unit, "cap_do", None) == 3:
        return file_unit.id in team_ids

    return False
    
def _can_user_view_file(
    *,
    db: Session,
    current_unit_id: Optional[str],
    role_codes: Set[str],
    file_unit_id: Optional[str],
) -> bool:
    """
    Logic nhìn thấy file theo đúng yêu cầu:

    1) Admin / HĐTV: không thấy kho phòng/tổ
    2) File do PHÒNG phát hành:
       - user có đơn vị chính là PHÒNG đó thấy
       - user có đơn vị chính là TỔ con trực thuộc PHÒNG đó thấy
    3) File do TỔ phát hành:
       - user có đơn vị chính là TỔ đó thấy
       - user có đơn vị chính là PHÒNG mẹ và có role QL phòng thấy
       - nhân viên thuộc PHÒNG mẹ không thấy
    """
    if not current_unit_id or not file_unit_id:
        return False

    if _is_admin_or_leader(role_codes):
        return False

    current_unit = _get_unit(db, current_unit_id)
    file_unit = _get_unit(db, file_unit_id)

    if not current_unit or not file_unit:
        return False

    # File do PHÒNG phát hành
    if file_unit.cap_do == 2:
        if current_unit_id == file_unit.id:
            return True
        if current_unit.cap_do == 3 and current_unit.parent_id == file_unit.id:
            return True
        return False

    # File do TỔ phát hành
    if file_unit.cap_do == 3:
        if current_unit_id == file_unit.id:
            return True
        if (
            current_unit.cap_do == 2
            and current_unit.id == file_unit.parent_id
            and _is_room_manager(role_codes)
        ):
            return True
        return False

    # Cấp khác: không mở
    return False


def _ensure_view_access(
    rec: Files,
    user_id: str,
    role_codes: Set[str],
    db: Session
) -> None:
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    if _has_files_visibility_grant(db, user_id):
        return

    if not _can_user_view_file_by_membership(
        db=db,
        user_id=user_id,
        role_codes=role_codes,
        file_unit_id=rec.unit_id,
    ):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tệp này.")


def _ensure_delete_access(
    rec: Files,
    user_id: str,
    role_codes: Set[str],
    db: Session
) -> None:
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    if not _can_delete_file_by_membership(
        db=db,
        user_id=user_id,
        role_codes=role_codes,
        file_unit_id=rec.unit_id,
        owner_id=rec.owner_id,
    ):
        raise HTTPException(status_code=403, detail="Bạn không có quyền xóa tệp này.")


def _parse_positive_int(raw: str, default: int, min_value: int = 1, max_value: int = 1000) -> int:
    try:
        value = int(raw)
    except Exception:
        value = default
    if value < min_value:
        value = min_value
    if value > max_value:
        value = max_value
    return value


def _to_vietnam_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Chuẩn hóa datetime sang giờ Việt Nam (UTC+7) mà không phụ thuộc tzdata.

    Quy ước:
    - Nếu dt không có tzinfo => coi là UTC rồi cộng 7 giờ.
    - Nếu dt đã có tzinfo => chuyển về UTC rồi cộng 7 giờ.
    """
    if not dt:
        return None

    vn_offset = timedelta(hours=7)

    if dt.tzinfo is None:
        return dt + vn_offset

    dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt_utc + vn_offset


def _serialize_file_row(
    rec: Files,
    db: Session,
    user_id: str,
    role_codes: Set[str]
) -> dict:
    owner_name = ""
    if getattr(rec, "owner", None):
        owner_name = rec.owner.full_name or rec.owner.username or ""

    ext = _get_file_ext(rec.original_name)
    uploaded_at_vn = _to_vietnam_datetime(rec.uploaded_at)

    return {
        "id": rec.id,
        "original_name": rec.original_name,
        "mime_type": rec.mime_type or "",
        "size_bytes": rec.size_bytes or 0,
        "size_display": _format_size(rec.size_bytes),
        "uploaded_at": uploaded_at_vn,
        "uploaded_at_display": uploaded_at_vn.strftime("%d/%m/%Y %H:%M") if uploaded_at_vn else "",
        "owner_name": owner_name,
        "path": rec.path,
        "file_ext": ext[1:].upper() if ext else "",
        "file_kind": _get_file_kind(rec.original_name),
        "file_kind_label": _get_file_kind_label(rec.original_name),
        "can_preview_inline": _can_inline_preview(rec.original_name),
        "can_delete": _can_delete_file_by_membership(
            db=db,
            user_id=user_id,
            role_codes=role_codes,
            file_unit_id=rec.unit_id,
            owner_id=rec.owner_id,
        ),
    }


def _sort_rows(rows: list[dict], sort: str, direction: str) -> list[dict]:
    reverse = direction == "desc"

    if sort == "name":
        return sorted(rows, key=lambda x: (x["original_name"] or "").lower(), reverse=reverse)
    if sort == "type":
        return sorted(rows, key=lambda x: (x["file_kind_label"] or "").lower(), reverse=reverse)
    if sort == "owner":
        return sorted(rows, key=lambda x: (x["owner_name"] or "").lower(), reverse=reverse)
    if sort == "size":
        return sorted(rows, key=lambda x: int(x["size_bytes"] or 0), reverse=reverse)

    return sorted(
        rows,
        key=lambda x: x["uploaded_at"] or datetime.min,
        reverse=reverse
    )


def _paginate_rows(rows: list[dict], page: int, per_page: int) -> tuple[list[dict], int, int]:
    total = len(rows)
    total_pages = max(1, (total + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    return rows[start:end], total, total_pages


# =========================
# Routes
# =========================
@router.get("", include_in_schema=False)
def files_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)
    has_files_grant = _has_files_visibility_grant(db, user.id)
    
    keyword = (request.query_params.get("q") or "").strip()
    kind = (request.query_params.get("kind") or "all").strip().lower()
    sort = (request.query_params.get("sort") or "uploaded_at").strip().lower()
    direction = (request.query_params.get("direction") or "desc").strip().lower()
    page = _parse_positive_int(request.query_params.get("page", "1"), default=1)
    per_page = _parse_positive_int(request.query_params.get("per_page", "10"), default=10, min_value=5, max_value=100)

    if sort not in SORT_FIELDS:
        sort = "uploaded_at"
    if direction not in {"asc", "desc"}:
        direction = "desc"
    if kind not in {"all", "document", "spreadsheet", "pdf", "image", "video"}:
        kind = "all"

    rows: list[dict] = []

    # HĐTV, BGĐ: không thấy tab/kho tài liệu
    # Admin: thấy toàn bộ
    # User khác: lọc theo membership đúng cấu trúc Xét nghiệm
    if _is_system_admin(role_codes):
        query = (
            db.query(Files)
            .filter(Files.is_deleted == False)  # noqa: E712
            .order_by(Files.uploaded_at.desc())
        )

        if keyword:
            query = query.filter(func.lower(Files.original_name).like(f"%{keyword.lower()}%"))

        records = query.all()
        rows = [_serialize_file_row(rec, db, user.id, role_codes) for rec in records]

        if kind != "all":
            rows = [r for r in rows if r["file_kind"] == kind]

        rows = _sort_rows(rows, sort=sort, direction=direction)

    elif current_unit_id and not _is_files_tab_hidden(role_codes):
        query = (
            db.query(Files)
            .filter(Files.is_deleted == False)  # noqa: E712
            .order_by(Files.uploaded_at.desc())
        )

        if keyword:
            query = query.filter(func.lower(Files.original_name).like(f"%{keyword.lower()}%"))

        records = query.all()

        if not has_files_grant:
            records = [
                rec for rec in records
                if _can_user_view_file_by_membership(
                    db=db,
                    user_id=user.id,
                    role_codes=role_codes,
                    file_unit_id=rec.unit_id,
                )
            ]

        rows = [_serialize_file_row(rec, db, user.id, role_codes) for rec in records]

        if kind != "all":
            rows = [r for r in rows if r["file_kind"] == kind]

        rows = _sort_rows(rows, sort=sort, direction=direction)

    paged_rows, total_files, total_pages = _paginate_rows(rows, page=page, per_page=per_page)

    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
            "company_name": getattr(settings, "COMPANY_NAME", ""),
            "rows": paged_rows,
            "msg": request.query_params.get("msg", ""),
            "error": request.query_params.get("error", ""),
            "q": keyword,
            "kind": kind,
            "sort": sort,
            "direction": direction,
            "page": page,
            "per_page": per_page,
            "total_files": total_files,
            "total_pages": total_pages,
            "accept_types": ",".join(sorted(ALLOWED_EXTENSIONS)),
            "is_hidden_for_admin_or_leader": _is_files_tab_hidden(role_codes),
            "upload_units": _get_uploadable_units(db, user.id, role_codes),
            "current_unit_id": current_unit_id or "",
        }
    )


@router.post("/upload")
async def upload_file(
    request: Request,
    linked_object_type: str = Form("DOC"),
    linked_object_id: str = Form(""),
    unit_id: str = Form(""),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    if _is_files_tab_hidden(role_codes):
        return RedirectResponse(
            url="/files?error=HĐTV hoặc BGĐ không sử dụng kho tài liệu của Khoa Xét nghiệm.",
            status_code=302,
        )

    if not current_unit_id:
        return RedirectResponse(
            url="/files?error=Tài khoản chưa được gán đơn vị chính, không thể tải tệp lên.",
            status_code=302,
        )

    if not upfile or not upfile.filename:
        return RedirectResponse(
            url="/files?error=Chưa chọn tệp để tải lên.",
            status_code=302,
        )

    if not _is_allowed_extension(upfile.filename):
        return RedirectResponse(
            url="/files?error=Định dạng tệp không được hỗ trợ.",
            status_code=302,
        )

    target_unit_id = (unit_id or current_unit_id or "").strip()
    if not _can_upload_to_unit(db, user.id, role_codes, target_unit_id):
        return RedirectResponse(
            url="/files?error=Bạn không có quyền tải tài liệu lên đơn vị đã chọn.",
            status_code=302,
        )

    target_unit = _get_unit(db, target_unit_id)
    if not target_unit:
        return RedirectResponse(
            url="/files?error=Đơn vị tải tài liệu không hợp lệ.",
            status_code=302,
        )

    max_bytes = _get_max_file_bytes()

    sub_parts = [str(target_unit_id), str(user.id)]
    if linked_object_type:
        sub_parts.append(linked_object_type.strip())
    if linked_object_id:
        sub_parts.append(linked_object_id.strip())

    dest_dir = os.path.join(_get_upload_dir(), *sub_parts)
    dest, size, _sha = _save_upload(dest_dir, upfile)

    if size > max_bytes:
        try:
            os.remove(dest)
        except Exception:
            pass
        return RedirectResponse(
            url="/files?error=File vượt quá dung lượng cho phép.",
            status_code=302,
        )

    mime_type = _guess_mime(dest)

    rec = Files(
        original_name=upfile.filename,
        path=dest,
        mime_type=mime_type,
        size_bytes=size,
        owner_id=user.id,
        unit_id=target_unit_id,
    )
    db.add(rec)
    db.commit()

    return RedirectResponse(
        url="/files?msg=Tải tệp lên thành công.",
        status_code=302,
    )


@router.get("/download/{file_id}")
def download_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_view_access(rec, user.id, role_codes, db)

    if not rec.path or not os.path.exists(rec.path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    filename = rec.original_name or os.path.basename(rec.path)
    media_type = rec.mime_type or "application/octet-stream"

    try:
        return FileResponse(rec.path, filename=filename, media_type=media_type)
    except TypeError:
        return FileResponse(
            rec.path,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )


@router.get("/view/{file_id}")
def view_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_view_access(rec, user.id, role_codes, db)

    if not rec.path or not os.path.exists(rec.path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    filename = rec.original_name or os.path.basename(rec.path)
    media_type = rec.mime_type or "application/octet-stream"

    return FileResponse(
        rec.path,
        media_type=media_type,
        headers={"Content-Disposition": f'inline; filename="{filename}"'}
    )


@router.post("/delete/{file_id}")
def delete_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_delete_access(rec, user.id, role_codes, db)

    rec.is_deleted = True
    db.add(rec)
    db.commit()

    try:
        if rec.path and os.path.exists(rec.path):
            os.remove(rec.path)
    except Exception:
        pass

    return RedirectResponse(
        url="/files?msg=Xóa tệp thành công.",
        status_code=302,
    )