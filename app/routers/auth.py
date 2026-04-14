from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func
from pathlib import Path
from typing import Optional, List, Tuple
import logging
import os
import sys
import json

from app.database import get_db
from app.models import Users, UserStatus, UnitStatus, Units, UserUnitMemberships, Roles, UserRoles
from app.security.crypto import hash_password, verify_password

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")
logger = logging.getLogger(__name__)

POSITION_DEFS = {
    "NHAN_VIEN": {
        "label": "Nhân viên",
        "role_code": "ROLE_NHAN_VIEN",
        "requires_secret": False,
        "unit_modes": {2, 3},
    },
    "HOI_DONG": {
        "label": "HĐTV",
        "role_code": "ROLE_LANH_DAO",
        "requires_secret": True,
        "unit_modes": {1},
    },
    "BGD": {
        "label": "BGĐ",
        "role_code": "ROLE_BGD",
        "requires_secret": True,
        "unit_modes": {1},
    },
    "TRUONG_KHOA": {
        "label": "Trưởng khoa",
        "role_code": "ROLE_TRUONG_KHOA",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "PHO_TRUONG_KHOA": {
        "label": "Phó khoa",
        "role_code": "ROLE_PHO_TRUONG_KHOA",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "KY_THUAT_VIEN_TRUONG": {
        "label": "Kỹ thuật viên trưởng",
        "role_code": "ROLE_KY_THUAT_VIEN_TRUONG",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_CHAT_LUONG": {
        "label": "Quản lý chất lượng",
        "role_code": "ROLE_QL_CHAT_LUONG",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_KY_THUAT": {
        "label": "Quản lý kỹ thuật",
        "role_code": "ROLE_QL_KY_THUAT",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_AN_TOAN": {
        "label": "Quản lý an toàn",
        "role_code": "ROLE_QL_AN_TOAN",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_VAT_TU": {
        "label": "Quản lý vật tư",
        "role_code": "ROLE_QL_VAT_TU",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_TRANG_THIET_BI": {
        "label": "Quản lý trang thiết bị",
        "role_code": "ROLE_QL_TRANG_THIET_BI",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_MOI_TRUONG": {
        "label": "Quản lý môi trường",
        "role_code": "ROLE_QL_MOI_TRUONG",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_CNTT": {
        "label": "Quản lý CNTT",
        "role_code": "ROLE_QL_CNTT",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_CONG_VIEC": {
        "label": "Quản lý công việc",
        "role_code": "ROLE_QL_CONG_VIEC",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "TRUONG_NHOM": {
        "label": "Nhóm/Tổ trưởng",
        "role_code": "ROLE_TRUONG_NHOM",
        "requires_secret": True,
        "unit_modes": {3},
    },
    "PHO_NHOM": {
        "label": "Nhóm/Tổ phó",
        "role_code": "ROLE_PHO_NHOM",
        "requires_secret": True,
        "unit_modes": {3},
    },

    # Tương thích ngược - không hiển thị trên giao diện đăng ký
    "QL_PHONG": {
        "label": "Quản lý cấp phòng",
        "role_code": "ROLE_TRUONG_PHONG",
        "requires_secret": True,
        "unit_modes": {2},
    },
    "QL_TO": {
        "label": "Quản lý cấp tổ",
        "role_code": "ROLE_TO_TRUONG",
        "requires_secret": True,
        "unit_modes": {3},
    },
}

# -------------------------
# Helpers (dùng comment thay docstring để tránh SyntaxError)
# -------------------------

def _get_or_create_role(db: Session, code: str) -> Roles:
    # Tìm role theo code (Enum hoặc str). Không có thì tạo mới.
    code_up = str(getattr(code, "value", code)).upper()
    role = db.query(Roles).filter(func.upper(func.coalesce(Roles.code, "")) == code_up).first()
    if role:
        return role
    role = Roles(code=code_up, name=code_up) if hasattr(Roles, "name") else Roles(code=code_up)
    db.add(role)
    db.commit()
    db.refresh(role)
    return role


def _assign_role_if_missing(db: Session, user: Users, code: str) -> None:
    # Gán role cho user nếu chưa có
    role = _get_or_create_role(db, code)
    exists = db.query(UserRoles).filter(
        UserRoles.user_id == user.id,
        UserRoles.role_id == role.id
    ).first()
    if not exists:
        db.add(UserRoles(user_id=user.id, role_id=role.id))
        db.commit()


def _add_membership_if_missing(db: Session, user: Users, unit_id: str) -> None:
    # Gán membership đơn vị nếu chưa có
    exists = db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.unit_id == unit_id
    ).first()
    if not exists:
        db.add(UserUnitMemberships(user_id=user.id, unit_id=unit_id))
        db.commit()


def _load_role_codes_for_user(db: Session, user_id: str) -> List[str]:
    # Lấy danh sách mã quyền cho user, trả về list[str] UPPER
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    codes: List[str] = []
    for (c,) in rows:
        code_up = str(getattr(c, "value", c)).upper() if c is not None else ""
        if code_up:
            codes.append(code_up)
    return codes


def _write_role_flags_to_session(request: Request, role_codes: List[str]) -> None:
    # Ghi các khóa session phục vụ giao diện (menu)
    # - roles: list[str]
    # - is_admin: bool (ROLE_ADMIN)
    # - is_admin_or_leader: bool (ROLE_ADMIN hoặc ROLE_LANH_DAO)
    request.session["roles"] = role_codes
    is_admin = "ROLE_ADMIN" in role_codes
    is_admin_or_leader = bool(set(role_codes) & {"ROLE_ADMIN", "ROLE_LANH_DAO"})
    request.session["is_admin"] = is_admin
    request.session["is_admin_or_leader"] = is_admin_or_leader


# -------------------------
# Secret code helpers
# Đồng bộ contract dữ liệu với account_secrets.py
# -------------------------

def _runtime_root_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


RUNTIME_ROOT_DIR = _runtime_root_dir()
APP_DATA_DIR = os.path.join(RUNTIME_ROOT_DIR, "data")
DEFAULT_KEYS_DIR = os.path.join(APP_DATA_DIR, "keys")
STORE_CONFIG_PATH = os.path.join(APP_DATA_DIR, "secret_store_config.json")


def _normalize_storage_dir(raw: Optional[str]) -> str:
    val = (raw or "").strip()
    if not val:
        return DEFAULT_KEYS_DIR
    return os.path.abspath(os.path.expanduser(val))


def _load_store_config() -> dict:
    try:
        if os.path.exists(STORE_CONFIG_PATH):
            with open(STORE_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def _get_storage_dir() -> str:
    cfg = _load_store_config()
    return _normalize_storage_dir(cfg.get("storage_dir"))


def _get_secret_store_paths() -> dict:
    storage_dir = _get_storage_dir()
    return {
        "storage_dir": storage_dir,
        "global_key_path": os.path.join(storage_dir, "position_secret.key"),
        "unit_keys_path": os.path.join(storage_dir, "position_secrets.json"),
    }


def _load_global_secret() -> str:
    paths = _get_secret_store_paths()
    path = paths["global_key_path"]
    try:
        if os.path.exists(path):
            return (Path(path).read_text(encoding="utf-8").strip())
    except Exception:
        return ""
    return ""


def _load_unit_secret_map() -> dict:
    paths = _get_secret_store_paths()
    path = paths["unit_keys_path"]
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def _extract_secret_from_unit_entry(raw) -> str:
    """
    Hỗ trợ cả 2 kiểu dữ liệu:
    - Kiểu cũ: "unit_id": "ABC123"
    - Kiểu mới: "unit_id": {"secret": "ABC123", ...}
    """
    if isinstance(raw, dict):
        return str(raw.get("secret", "") or "").strip()
    return str(raw or "").strip()
    
    
def _resolve_target_unit_for_secret(
    db: Session,
    position: str,
    unit_id: Optional[str],
) -> Optional[Units]:
    """
    Xác định đúng đơn vị phải kiểm khóa:
    - HOI_DONG: không dùng unit
    - QL_PHONG: dùng chính PHÒNG; nếu chọn nhầm Tổ thì quy về PHÒNG cha
    - QL_TO: dùng chính TỔ; không quy về PHÒNG cha
    """
    pos = (position or "").upper().strip()
    if not unit_id:
        return None

    unit = db.get(Units, unit_id)
    if not unit:
        return None

    cap = getattr(unit, "cap_do", None)
    parent_id = getattr(unit, "parent_id", None)

    if pos == "QL_PHONG":
        if cap == 2:
            return unit
        if cap == 3 and parent_id:
            parent = db.get(Units, parent_id)
            if parent and getattr(parent, "cap_do", None) == 2:
                return parent
        return None

    if pos == "QL_TO":
        if cap == 3:
            return unit
        return None

    return None


def _validate_position_secret(
    db: Session,
    position: str,
    unit_id: Optional[str],
    secret_code: Optional[str],
) -> Tuple[bool, Optional[str]]:
    pos = (position or "").upper().strip()
    entered = (secret_code or "").strip()
    notice = "Xin hãy liên lạc admin để nhận mã khóa"
    pos_def = POSITION_DEFS.get(pos)

    if not pos_def:
        return False, "Vị trí đăng ký không hợp lệ."

    if not pos_def.get("requires_secret"):
        return True, None

    if not entered:
        return False, f"Khóa bí mật bắt buộc với vị trí đã chọn. {notice}"

    if pos in {"BGD", "HOI_DONG"}:
        global_secret = _load_global_secret()
        if not global_secret:
            return False, f"Hệ thống chưa được cấu hình khóa Global. {notice}"
        if entered != global_secret:
            return False, f"Khóa bí mật không đúng. {notice}"
        return True, None

    unit = db.get(Units, unit_id) if unit_id else None
    if not unit:
        return False, "Không xác định được đơn vị để kiểm tra khóa bí mật."

    allowed_caps = set(pos_def.get("unit_modes") or [])
    cap = getattr(unit, "cap_do", None)
    if allowed_caps and cap not in allowed_caps:
        return False, "Đơn vị đã chọn không phù hợp với vị trí đăng ký."

    # Quy tắc kiểm khóa:
    # - HĐTV, BGĐ: dùng khóa Global
    # - Trưởng/Phó khoa, KTV trưởng, QL chức năng, QL công việc: dùng khóa của đơn vị cấp 2 (Khoa)
    # - Nhóm trưởng, Nhóm phó: dùng khóa của đơn vị cấp 3 (Nhóm)
    # Với cấu trúc hiện tại, role đã bị chặn theo unit_modes nên chỉ cần lấy khóa đúng unit đang chọn.
    unit_secret_map = _load_unit_secret_map()
    target_secret = _extract_secret_from_unit_entry(unit_secret_map.get(unit.id))

    if not target_secret:
        return False, f"Đơn vị '{getattr(unit, 'ten_don_vi', '')}' chưa được cấp mã khóa. {notice}"

    if entered != target_secret:
        return False, f"Khóa bí mật không đúng. {notice}"

    return True, None


def _load_register_units(db: Session):
    rows = (
        db.query(Units.id, Units.ten_don_vi.label("ten_don_vi"), Units.cap_do.label("cap_do"))
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )
    result = []
    for r in rows:
        cap = int(r.cap_do or 0)
        if cap == 1:
            unit_type_label = "HĐTV"
        elif cap == 2:
            unit_type_label = "Khoa Xét nghiệm"
        elif cap == 3:
            unit_type_label = "Nhóm/Tổ"
        else:
            unit_type_label = f"Cấp {cap}"
        result.append({
            "id": r.id,
            "ten_don_vi": r.ten_don_vi,
            "cap_do": cap,
            "unit_type_label": unit_type_label,
        })
    return result


def _build_register_positions(units):
    available_caps = {int(u.get("cap_do") or 0) for u in (units or [])}
    items = []
    for code, cfg in POSITION_DEFS.items():
        if code in {"QL_PHONG", "QL_TO"}:
            continue
        allowed = set(cfg.get("unit_modes") or [])
        if allowed and not (available_caps & allowed):
            continue
        items.append({
            "code": code,
            "label": cfg.get("label", code),
            "requires_secret": bool(cfg.get("requires_secret")),
            "unit_modes": sorted(list(allowed)),
        })
    order = [
        "HOI_DONG", "BGD",
        "TRUONG_KHOA", "PHO_TRUONG_KHOA", "KY_THUAT_VIEN_TRUONG",
        "QL_CHAT_LUONG", "QL_KY_THUAT", "QL_AN_TOAN",
        "QL_VAT_TU", "QL_TRANG_THIET_BI", "QL_MOI_TRUONG", "QL_CNTT", "QL_CONG_VIEC",
        "TRUONG_NHOM", "PHO_NHOM", "NHAN_VIEN",
    ]
    idx = {code: i for i, code in enumerate(order)}
    items.sort(key=lambda x: (idx.get(x["code"], 999), x["label"]))
    return items


def _render_register(request: Request, db: Session, error: Optional[str] = None, form_data: Optional[dict] = None, status_code: int = 200):
    units = _load_register_units(db)
    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "units": units,
            "positions": _build_register_positions(units),
            "error": error,
            "app_name": "QLCV",
            "company_name": "Hùng Vương Gia Lai",
            "secret_notice": "Xin hãy liên lạc admin để nhận mã khóa",
            "form_data": form_data or {},
        },
        status_code=status_code,
    )


# -------------------------
# Login / Logout
# -------------------------

@router.get("/login", response_class=HTMLResponse)
def login_get(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@router.post("/login")
def login_post(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    u = db.query(Users).filter(Users.username == username).first()
    if not u or not verify_password(password, u.password_hash):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Sai tài khoản hoặc mật khẩu"},
            status_code=401
        )
    if u.status != UserStatus.ACTIVE:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Tài khoản chưa hoạt động"},
            status_code=403
        )

    # set session (tương thích ngược)
    request.session["user_id"] = u.id
    request.session["username"] = u.username

    # Nạp role codes và ghi vào session để menu hiển thị đúng
    role_codes = _load_role_codes_for_user(db, u.id)
    _write_role_flags_to_session(request, role_codes)

    # Điều hướng về dashboard (main.py sẽ xử lý điều hướng mềm nếu cần)
    return RedirectResponse(url="/dashboard", status_code=302)


@router.get("/logout")
def logout(request: Request):
    # Xóa session và điều hướng về trang đăng nhập
    request.session.clear()
    # 307 để giữ semantics nếu logout từ POST; GET vẫn hoạt động bình thường
    return RedirectResponse(url="/login", status_code=307)


# -------------------------
# Register (nếu hệ thống dùng; giữ nguyên URL)
# -------------------------

@router.get("/register", response_class=HTMLResponse)
def register_get(request: Request, db: Session = Depends(get_db)):
    return _render_register(request, db)


@router.post("/register", response_class=HTMLResponse)
def register_post(
    request: Request,
    full_name: str = Form(...),
    username: str = Form(...),
    email: Optional[str] = Form(None),
    phone: Optional[str] = Form(None),
    password: str = Form(...),
    confirm_password: str = Form(...),
    unit_id: Optional[str] = Form(None),
    position: str = Form(...),  # position code theo POSITION_DEFS
    secret_code: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    # Validate cơ bản
    if password != confirm_password:
        return _render_register(
            request, db,
            error="Mật khẩu nhập lại không khớp",
            form_data={
                "full_name": full_name, "username": username, "email": email or "", "phone": phone or "",
                "unit_id": unit_id or "", "position": position or "NHAN_VIEN", "secret_code": secret_code or "",
            },
            status_code=400,
        )
    if db.query(Users).filter(Users.username == username).first():
        return _render_register(
            request, db,
            error="Tên đăng nhập đã tồn tại",
            form_data={
                "full_name": full_name, "username": username, "email": email or "", "phone": phone or "",
                "unit_id": unit_id or "", "position": position or "NHAN_VIEN", "secret_code": secret_code or "",
            },
            status_code=400,
        )

    # Xác thực thật secret_code cho các vị trí nhạy cảm
    pos = (position or "").upper()
    ok_secret, secret_error = _validate_position_secret(db, pos, unit_id, secret_code)
    if not ok_secret:
        return _render_register(
            request, db,
            error=secret_error,
            form_data={
                "full_name": full_name, "username": username, "email": email or "", "phone": phone or "",
                "unit_id": unit_id or "", "position": position or "NHAN_VIEN", "secret_code": secret_code or "",
            },
            status_code=400,
        )

    # Tạo user
    u = Users(
        full_name=full_name,
        username=username,
        email=email,
        phone=phone,
        password_hash=hash_password(password),
        status=UserStatus.ACTIVE,
    )
    db.add(u)
    db.commit()
    db.refresh(u)

    # Gán role theo vị trí (ưu tiên bộ role mới, vẫn giữ tương thích ngược)
    pos_def = POSITION_DEFS.get(pos) or POSITION_DEFS["NHAN_VIEN"]
    _assign_role_if_missing(db, u, pos_def.get("role_code", "ROLE_NHAN_VIEN"))

    # Gán membership đơn vị
    if unit_id:
        unit = db.get(Units, unit_id)
        if unit:
            try:
                cap = int(getattr(unit, "cap_do", 0) or 0)
                parent_id = getattr(unit, "parent_id", None)
            except Exception:
                cap, parent_id = 0, None

            # BGĐ và lãnh đạo cấp cao: giữ đơn vị được chọn để hiển thị/phân loại nội bộ
            if pos in {"BGD", "HOI_DONG", "TRUONG_KHOA", "PHO_TRUONG_KHOA",
                       "QL_CHAT_LUONG", "QL_KY_THUAT", "QL_AN_TOAN",
                       "QL_VAT_TU", "QL_TRANG_THIET_BI", "QL_MOI_TRUONG", "QL_CNTT"}:
                _add_membership_if_missing(db, u, unit.id)
                if cap == 3 and parent_id:
                    _add_membership_if_missing(db, u, parent_id)

            elif pos in {"QL_PHONG"}:
                if cap == 3 and parent_id:
                    _add_membership_if_missing(db, u, parent_id)
                else:
                    _add_membership_if_missing(db, u, unit.id)

            elif pos in {"QL_TO", "TRUONG_NHOM", "PHO_NHOM"}:
                _add_membership_if_missing(db, u, unit.id)
                if cap == 3 and parent_id:
                    _add_membership_if_missing(db, u, parent_id)

            else:
                _add_membership_if_missing(db, u, unit.id)
                if cap == 3 and parent_id:
                    _add_membership_if_missing(db, u, parent_id)

    # set session sau khi đăng ký để vào thẳng hệ thống
    request.session["user_id"] = u.id
    request.session["username"] = u.username

    # Nạp role codes và ghi vào session để menu hiển thị đúng
    role_codes = _load_role_codes_for_user(db, u.id)
    _write_role_flags_to_session(request, role_codes)

    # Điều hướng về trang giao việc (giữ nguyên route)
    return RedirectResponse(url="/tasks", status_code=302)
