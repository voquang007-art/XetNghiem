from fastapi import APIRouter, Request, Depends, Form, HTTPException, status, Query
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import os

from ..security.deps import get_db, login_required, user_has_any_role
from ..models import (
    Users, UserStatus, Roles, RoleCode, Tasks,
    UserRoles, UserUnitMemberships, Units
)
from ..config import settings
from ..security.crypto import verify_password, hash_password

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


# ================== TIỆN ÍCH QUYỀN ==================
def _require_admin_or_leader(user: Users, db: Session):
    if not user_has_any_role(
        user,
        db,
        [
            RoleCode.ROLE_ADMIN,
            RoleCode.ROLE_LANH_DAO,
            RoleCode.ROLE_BGD,
            RoleCode.ROLE_TRUONG_KHOA,
            RoleCode.ROLE_PHO_TRUONG_KHOA,
        ],
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ Admin hoặc lãnh đạo được truy cập."
        )


# Nhóm role “vị trí” cho phép điều chỉnh
_POSITION_ROLE_CODES = [
    RoleCode.ROLE_LANH_DAO,
    RoleCode.ROLE_BGD,
    RoleCode.ROLE_TRUONG_KHOA,
    RoleCode.ROLE_PHO_TRUONG_KHOA,
    RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    RoleCode.ROLE_QL_CHAT_LUONG,
    RoleCode.ROLE_QL_KY_THUAT,
    RoleCode.ROLE_QL_AN_TOAN,
    RoleCode.ROLE_QL_VAT_TU,
    RoleCode.ROLE_QL_TRANG_THIET_BI,
    RoleCode.ROLE_QL_MOI_TRUONG,
    RoleCode.ROLE_QL_CNTT,
    RoleCode.ROLE_TRUONG_NHOM,
    RoleCode.ROLE_PHO_NHOM,
    RoleCode.ROLE_NHAN_VIEN,
]

_POSITION_MAP = {
    "LANH_DAO": RoleCode.ROLE_LANH_DAO,
    "BGD": RoleCode.ROLE_BGD,
    "TRUONG_KHOA": RoleCode.ROLE_TRUONG_KHOA,
    "PHO_TRUONG_KHOA": RoleCode.ROLE_PHO_TRUONG_KHOA,
    "KY_THUAT_VIEN_TRUONG": RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    "QL_CHAT_LUONG": RoleCode.ROLE_QL_CHAT_LUONG,
    "QL_KY_THUAT": RoleCode.ROLE_QL_KY_THUAT,
    "QL_AN_TOAN": RoleCode.ROLE_QL_AN_TOAN,
    "QL_VAT_TU": RoleCode.ROLE_QL_VAT_TU,
    "QL_TRANG_THIET_BI": RoleCode.ROLE_QL_TRANG_THIET_BI,
    "QL_MOI_TRUONG": RoleCode.ROLE_QL_MOI_TRUONG,
    "QL_CNTT": RoleCode.ROLE_QL_CNTT,
    "TRUONG_NHOM": RoleCode.ROLE_TRUONG_NHOM,
    "PHO_NHOM": RoleCode.ROLE_PHO_NHOM,
    "NHAN_VIEN": RoleCode.ROLE_NHAN_VIEN,
}

_POSITION_LABELS = {
    "LANH_DAO": "HĐTV",
    "BGD": "BGĐ",
    "TRUONG_KHOA": "Trưởng khoa",
    "PHO_TRUONG_KHOA": "Phó khoa",
    "KY_THUAT_VIEN_TRUONG": "Kỹ thuật viên trưởng",
    "QL_CHAT_LUONG": "Quản lý chất lượng",
    "QL_KY_THUAT": "Quản lý kỹ thuật",
    "QL_AN_TOAN": "Quản lý an toàn",
    "QL_VAT_TU": "Quản lý vật tư",
    "QL_TRANG_THIET_BI": "Quản lý trang thiết bị",
    "QL_MOI_TRUONG": "Quản lý môi trường",
    "QL_CNTT": "Quản lý CNTT",
    "TRUONG_NHOM": "Nhóm/Tổ trưởng",
    "PHO_NHOM": "Nhóm/Tổ phó",
    "NHAN_VIEN": "Nhân viên",
}

_ROLECODE_TO_POSITION_KEY = {
    str(RoleCode.ROLE_LANH_DAO): "LANH_DAO",
    str(RoleCode.ROLE_BGD): "BGD",
    str(RoleCode.ROLE_TRUONG_KHOA): "TRUONG_KHOA",
    str(RoleCode.ROLE_PHO_TRUONG_KHOA): "PHO_TRUONG_KHOA",
    str(RoleCode.ROLE_KY_THUAT_VIEN_TRUONG): "KY_THUAT_VIEN_TRUONG",
    str(RoleCode.ROLE_QL_CHAT_LUONG): "QL_CHAT_LUONG",
    str(RoleCode.ROLE_QL_KY_THUAT): "QL_KY_THUAT",
    str(RoleCode.ROLE_QL_AN_TOAN): "QL_AN_TOAN",
    str(RoleCode.ROLE_QL_VAT_TU): "QL_VAT_TU",
    str(RoleCode.ROLE_QL_TRANG_THIET_BI): "QL_TRANG_THIET_BI",
    str(RoleCode.ROLE_QL_MOI_TRUONG): "QL_MOI_TRUONG",
    str(RoleCode.ROLE_QL_CNTT): "QL_CNTT",
    str(RoleCode.ROLE_TRUONG_NHOM): "TRUONG_NHOM",
    str(RoleCode.ROLE_PHO_NHOM): "PHO_NHOM",
    str(RoleCode.ROLE_NHAN_VIEN): "NHAN_VIEN",
}


def _get_role_code_of_user(db: Session, user: Users):
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user.id)
        .all()
    )
    allowed = {str(getattr(rc, "value", rc)).upper() for rc in _POSITION_ROLE_CODES}
    for (role_code,) in rows:
        code_str = str(getattr(role_code, "value", role_code)).upper()
        if code_str in allowed:
            return role_code
    return None


def _rebuild_user_memberships_for_position(
    db: Session,
    user: Users,
    role_code: RoleCode,
    primary_unit_id: str
) -> None:
    """
    Chuẩn hoá membership hiện hành theo đúng vị trí + đơn vị hiện tại.
    Nguyên tắc:
    - Không giữ membership lịch sử trong bảng hiện hành.
    - Xoá toàn bộ membership cũ rồi dựng lại membership mới.
    - HĐTV/BGĐ: giữ đơn vị chính để hiển thị.
    - Quản lý khoa / KTV trưởng / quản lý chức năng: thuộc Khoa chính.
    - Nhóm/Tổ trưởng, Nhóm/Tổ phó: thuộc Tổ chính + Khoa cha.
    - Nhân viên thuộc Tổ: thuộc Tổ chính + Khoa cha.
    - Nhân viên thuộc Khoa: chỉ thuộc Khoa chính.
    """
    unit = db.get(Units, primary_unit_id)
    if not unit:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị chính.")

    db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id
    ).delete(synchronize_session=False)

    memberships_to_add = []

    if role_code in {RoleCode.ROLE_LANH_DAO, RoleCode.ROLE_BGD}:
        memberships_to_add.append((primary_unit_id, True))

    elif role_code in {
        RoleCode.ROLE_TRUONG_KHOA,
        RoleCode.ROLE_PHO_TRUONG_KHOA,
        RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
        RoleCode.ROLE_QL_CHAT_LUONG,
        RoleCode.ROLE_QL_KY_THUAT,
        RoleCode.ROLE_QL_AN_TOAN,
        RoleCode.ROLE_QL_VAT_TU,
        RoleCode.ROLE_QL_TRANG_THIET_BI,
        RoleCode.ROLE_QL_MOI_TRUONG,
        RoleCode.ROLE_QL_CNTT,
    }:
        if int(unit.cap_do) != 2:
            raise HTTPException(
                status_code=400,
                detail="Vị trí này phải thuộc đơn vị cấp Khoa."
            )
        memberships_to_add.append((primary_unit_id, True))

    elif role_code in {RoleCode.ROLE_TRUONG_NHOM, RoleCode.ROLE_PHO_NHOM}:
        if int(unit.cap_do) != 3:
            raise HTTPException(
                status_code=400,
                detail="Nhóm/Tổ trưởng hoặc Nhóm/Tổ phó phải thuộc đơn vị cấp Nhóm/Tổ."
            )
        memberships_to_add.append((primary_unit_id, True))
        if unit.parent_id:
            memberships_to_add.append((unit.parent_id, False))

    elif role_code == RoleCode.ROLE_NHAN_VIEN:
        if int(unit.cap_do) == 3:
            memberships_to_add.append((primary_unit_id, True))
            if unit.parent_id:
                memberships_to_add.append((unit.parent_id, False))
        elif int(unit.cap_do) == 2:
            memberships_to_add.append((primary_unit_id, True))
        else:
            memberships_to_add.append((primary_unit_id, True))

    else:
        memberships_to_add.append((primary_unit_id, True))

    seen = set()
    for unit_id, is_primary in memberships_to_add:
        if not unit_id or unit_id in seen:
            continue
        seen.add(unit_id)
        db.add(
            UserUnitMemberships(
                user_id=user.id,
                unit_id=unit_id,
                is_primary=is_primary
            )
        )


def _set_user_position(db: Session, user: Users, role_code: RoleCode) -> None:
    role_ids = [r.id for r in db.query(Roles).filter(Roles.code.in_(_POSITION_ROLE_CODES)).all()]
    if role_ids:
        db.query(UserRoles).filter(
            UserRoles.user_id == user.id,
            UserRoles.role_id.in_(role_ids)
        ).delete(synchronize_session=False)

    role_obj = db.query(Roles).filter(Roles.code == role_code).first()
    if not role_obj:
        raise HTTPException(status_code=400, detail="Mã vị trí không hợp lệ trong hệ thống.")
    db.add(UserRoles(user_id=user.id, role_id=role_obj.id))


def _transfer_user_unit(db: Session, user: Users, new_unit_id: str) -> None:
    """
    Điều chuyển đơn vị chính:
    - Cập nhật đơn vị chính nếu Users có field lưu trực tiếp.
    - Đồng thời CHUẨN HOÁ LẠI toàn bộ membership hiện hành theo vị trí hiện tại.
    - Không giữ membership lịch sử của đơn vị cũ trong bảng membership hiện hành.
    """
    if hasattr(user, "don_vi_chinh_id"):
        user.don_vi_chinh_id = new_unit_id
    elif hasattr(user, "unit_id"):
        user.unit_id = new_unit_id

    current_role_code = _get_role_code_of_user(db, user)
    if not current_role_code:
        raise HTTPException(
            status_code=400,
            detail="Người dùng chưa có vị trí hiện hành để chuẩn hoá đơn vị."
        )

    _rebuild_user_memberships_for_position(db, user, current_role_code, new_unit_id)


def _decorate_manage_users(db: Session, users: list[Users]) -> list[Users]:
    if not users:
        return users

    user_ids = [u.id for u in users]
    if not user_ids:
        return users

    role_rows = (
        db.query(UserRoles.user_id, Roles.code, Roles.name)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(UserRoles.user_id.in_(user_ids))
        .filter(Roles.code.in_(_POSITION_ROLE_CODES))
        .all()
    )

    role_map = {}
    for user_id, role_code, role_name in role_rows:
        code_str = str(role_code)
        pos_key = _ROLECODE_TO_POSITION_KEY.get(code_str)
        if pos_key and user_id not in role_map:
            role_map[user_id] = {
                "position_key": pos_key,
                "position_label": _POSITION_LABELS.get(pos_key, role_name or code_str),
            }

    mem_rows = (
        db.query(
            UserUnitMemberships.user_id,
            UserUnitMemberships.unit_id,
            UserUnitMemberships.is_primary,
            Units.ten_don_vi,
            Units.cap_do,
        )
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id.in_(user_ids))
        .order_by(UserUnitMemberships.user_id.asc(), UserUnitMemberships.is_primary.desc())
        .all()
    )

    unit_map = {}
    for user_id, unit_id, is_primary, ten_don_vi, cap_do in mem_rows:
        if user_id not in unit_map:
            unit_map[user_id] = {
                "unit_id": unit_id,
                "unit_name": ten_don_vi or "",
                "unit_cap_do": cap_do,
            }

    for u in users:
        role_info = role_map.get(u.id, {})
        unit_info = unit_map.get(u.id, {})

        setattr(u, "current_position_key", role_info.get("position_key", ""))
        setattr(u, "current_position_label", role_info.get("position_label", "-"))
        setattr(u, "current_unit_id", unit_info.get("unit_id", ""))
        setattr(u, "current_unit_name", unit_info.get("unit_name", "-"))

    return users


# ================== HỒ SƠ CÁ NHÂN ==================
@router.get("/account")
def my_account(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    return templates.TemplateResponse("account.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "user": user
    })


@router.post("/account/change-password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    confirm_new_password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)

    current_password = (current_password or "").strip()
    new_password = (new_password or "").strip()
    confirm_new_password = (confirm_new_password or "").strip()

    if not current_password or not new_password or not confirm_new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Vui lòng nhập đầy đủ các trường đổi mật khẩu."
        }, status_code=400)

    if not verify_password(current_password, user.password_hash):
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu hiện tại không đúng."
        }, status_code=400)

    if new_password != confirm_new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu mới và xác nhận mật khẩu mới không khớp."
        }, status_code=400)

    if current_password == new_password:
        return templates.TemplateResponse("account.html", {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "user": user,
            "change_password_error": "Mật khẩu mới không được trùng mật khẩu hiện tại."
        }, status_code=400)

    user.password_hash = hash_password(new_password)
    db.add(user)
    db.commit()

    return templates.TemplateResponse("account.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "user": user,
        "change_password_success": "Đổi mật khẩu thành công."
    })


# ================== QUẢN TRỊ NGƯỜI DÙNG ==================
@router.get("/account/users")
def users_manage(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    _require_admin_or_leader(user, db)

    actives = (
        db.query(Users)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.created_at.desc())
        .all()
    )
    locked = (
        db.query(Users)
        .filter(Users.status == UserStatus.LOCKED)
        .order_by(Users.created_at.desc())
        .all()
    )
    all_units = (
        db.query(Units)
        .filter(func.upper(func.coalesce(Units.trang_thai, "")) == "ACTIVE")
        .order_by(Units.cap_do.asc(), Units.ten_don_vi.asc())
        .all()
    )

    actives = _decorate_manage_users(db, actives)
    locked = _decorate_manage_users(db, locked)

    return templates.TemplateResponse("users_manage.html", {
        "request": request,
        "app_name": settings.APP_NAME,
        "company_name": settings.COMPANY_NAME,
        "actives": actives,
        "locked": locked,
        "all_units": all_units
    })


# ----- KÍCH HOẠT / KHOÁ / MỞ / XOÁ -----
@router.post("/account/users/activate")
def activate_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    u.status = UserStatus.ACTIVE
    db.add(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


@router.post("/account/users/lock")
def lock_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    if u.id == me.id:
        raise HTTPException(status_code=400, detail="Không thể khóa chính mình.")
    u.status = UserStatus.LOCKED
    db.add(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


@router.post("/account/users/unlock")
def unlock_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    u.status = UserStatus.ACTIVE
    db.add(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


@router.post("/account/users/delete")
def delete_user(request: Request, user_id: str = Form(...), db: Session = Depends(get_db)):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    if u.id == me.id:
        raise HTTPException(status_code=400, detail="Không thể xóa chính mình.")
    if user_has_any_role(u, db, [RoleCode.ROLE_ADMIN, RoleCode.ROLE_LANH_DAO]):
        raise HTTPException(status_code=403, detail="Không được xóa tài khoản có vai trò Admin/Lãnh đạo.")

    ref1 = db.query(Tasks.id).filter(Tasks.created_by == u.id).first()
    assigned_field = getattr(Tasks, "assigned_to_user_id", None)
    ref2 = db.query(Tasks.id).filter(assigned_field == u.id).first() if assigned_field is not None else None
    if ref1 or ref2:
        raise HTTPException(status_code=400, detail="Không thể xóa: còn dữ liệu nhiệm vụ liên quan.")

    db.delete(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


# ----- ĐIỀU CHỈNH VỊ TRÍ -----
@router.post("/account/users/position")
def set_position(
    request: Request,
    user_id: str = Form(...),
    new_role: str = Form(...),
    db: Session = Depends(get_db)
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    role_code = _POSITION_MAP.get(new_role)
    if not role_code:
        raise HTTPException(status_code=400, detail="Giá trị vị trí không hợp lệ.")

    _set_user_position(db, u, role_code)

    current_primary = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == u.id, UserUnitMemberships.is_primary == True)  # noqa: E712
        .first()
    )
    if current_primary and current_primary[0]:
        _rebuild_user_memberships_for_position(db, u, role_code, current_primary[0])

    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


# ----- ĐIỀU CHUYỂN ĐƠN VỊ -----
@router.post("/account/users/unit-transfer")
def unit_transfer(
    request: Request,
    user_id: str = Form(...),
    new_unit_id: str = Form(...),
    db: Session = Depends(get_db)
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    un = db.get(Units, new_unit_id)
    if not un:
        raise HTTPException(status_code=404, detail="Không tìm thấy đơn vị.")
    _transfer_user_unit(db, u, new_unit_id)
    db.add(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)


# ----- SỬA THÔNG TIN (EDIT / UPDATE) -----
@router.get("/account/users/edit")
def edit_user_screen(request: Request, user_id: str = Query(...), db: Session = Depends(get_db)):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")
    return templates.TemplateResponse("user_edit.html", {"request": request, "u": u})


@router.post("/account/users/update")
def update_user(
    request: Request,
    user_id: str = Form(...),
    full_name: str = Form(""),
    email: str = Form(""),
    phone: str = Form(""),
    username: str = Form(...),
    db: Session = Depends(get_db),
):
    me = login_required(request, db)
    _require_admin_or_leader(me, db)
    u = db.get(Users, user_id)
    if not u:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    full_name = (full_name or "").strip()
    email = (email or "").strip()
    phone = (phone or "").strip()
    username = (username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Tên đăng nhập không được để trống.")

    if username != getattr(u, "username", ""):
        existed = db.query(Users).filter(and_(Users.username == username, Users.id != u.id)).first()
        if existed:
            raise HTTPException(status_code=400, detail="Tên đăng nhập đã tồn tại.")
        u.username = username

    if hasattr(u, "full_name"):
        u.full_name = full_name
    if hasattr(u, "email"):
        u.email = email
    if hasattr(u, "phone"):
        u.phone = phone

    db.add(u)
    db.commit()
    return RedirectResponse(url="/account/users", status_code=302)