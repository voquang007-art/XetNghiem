from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from sqlalchemy.orm import Session
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
import os, re

from ..security.deps import get_db, login_required, user_has_any_role
from ..models import Units, UnitStatus, RoleCode, UserUnitMemberships
from ..config import settings

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))
LAB_UNIT_NAME = "Khoa Xét nghiệm"

MANAGE_ROLE_CODES = [
    RoleCode.ROLE_ADMIN,
    RoleCode.ROLE_TRUONG_KHOA,
    RoleCode.ROLE_PHO_TRUONG_KHOA,
    RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
]


def _can_manage_units(user, db: Session) -> bool:
    return user_has_any_role(user, db, MANAGE_ROLE_CODES)


def _get_lab_unit(db: Session) -> Units | None:
    return (
        db.query(Units)
        .filter(
            Units.trang_thai == UnitStatus.ACTIVE,
            Units.cap_do == 2,
            Units.ten_don_vi == LAB_UNIT_NAME,
        )
        .order_by(Units.order_index, Units.ten_don_vi)
        .first()
    )


def _get_primary_unit_for_user(db: Session, user) -> Units | None:
    membership = (
        db.query(UserUnitMemberships)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(
            UserUnitMemberships.user_id == user.id,
            UserUnitMemberships.is_active.is_(True),
            UserUnitMemberships.is_primary.is_(True),
            Units.trang_thai == UnitStatus.ACTIVE,
        )
        .order_by(UserUnitMemberships.start_date.desc(), UserUnitMemberships.id.desc())
        .first()
    )
    return membership.unit if membership and membership.unit else None

def build_path(parent: Units | None, name: str) -> str:
    def slugify(s: str) -> str:
        s = s.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "-", s)
        return s.strip("-")
    base = "/org"
    if parent:
        return f"{parent.path}/{slugify(name)}"
    return f"{base}/{slugify(name)}"


def _rebuild_descendant_paths(db: Session, parent_unit: Units) -> None:
    """
    Cascade cập nhật path cho toàn bộ đơn vị con/cháu khi path của đơn vị cha thay đổi.
    Chỉ cập nhật path nhánh con; không tác động đơn vị cha của parent_unit.
    """
    children = (
        db.query(Units)
        .filter(Units.parent_id == parent_unit.id)
        .order_by(Units.cap_do, Units.order_index, Units.ten_don_vi)
        .all()
    )

    for child in children:
        child.path = build_path(parent_unit, child.ten_don_vi)
        db.add(child)
        _rebuild_descendant_paths(db, child)


def _has_active_children(db: Session, unit_id: str) -> bool:
    return (
        db.query(Units)
        .filter(Units.parent_id == unit_id, Units.trang_thai == UnitStatus.ACTIVE)
        .first()
        is not None
    )
    
def _room_options_for_form(db: Session) -> list[Units]:
    """
    Combobox đơn vị cha chỉ hiển thị duy nhất Khoa Xét nghiệm.
    """
    lab_unit = _get_lab_unit(db)
    return [lab_unit] if lab_unit else []
    
def _root_options_for_form(db: Session) -> list[Units]:
    """
    Không dùng trong màn hình cơ cấu của Khoa Xét nghiệm.
    Giữ hàm để tránh ảnh hưởng cấu trúc hiện tại.
    """
    return []
    
    
@router.get("")
def list_units(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)

    can_manage = _can_manage_units(user, db)
    lab_unit = _get_lab_unit(db)
    current_unit = _get_primary_unit_for_user(db, user)

    if can_manage:
        units = []
        if lab_unit:
            units.append(lab_unit)
            child_units = (
                db.query(Units)
                .filter(
                    Units.trang_thai == UnitStatus.ACTIVE,
                    Units.cap_do == 3,
                    Units.parent_id == lab_unit.id,
                )
                .order_by(Units.order_index, Units.ten_don_vi)
                .all()
            )
            units.extend(child_units)
    else:
        units = [current_unit] if current_unit else []

    parent_ids = {u.parent_id for u in units if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    unit_rows = []
    for u in units:
        parent_name = ""
        if getattr(u, "parent_id", None):
            p = parent_map.get(u.parent_id)
            if p and getattr(p, "ten_don_vi", None):
                parent_name = p.ten_don_vi

        if getattr(u, "cap_do", None) == 2:
            unit_type_label = "Khoa Xét nghiệm"
        elif getattr(u, "cap_do", None) == 3:
            unit_type_label = "Nhóm/Tổ"
        else:
            unit_type_label = f"Cấp {u.cap_do}"

        unit_rows.append({
            "id": u.id,
            "ten_don_vi": u.ten_don_vi,
            "cap_do": u.cap_do,
            "unit_type_label": unit_type_label,
            "parent_id": u.parent_id,
            "parent_name": parent_name,
            "path": u.path,
            "trang_thai": u.trang_thai.value if getattr(u, "trang_thai", None) else "",
        })

    root_options = _root_options_for_form(db)
    room_options = _room_options_for_form(db)

    return templates.TemplateResponse(
        "units.html",
        {
            "request": request,
            "app_name": settings.APP_NAME,
            "company_name": settings.COMPANY_NAME,
            "units": unit_rows,
            "root_options": root_options,
            "room_options": room_options,
            "can_manage": can_manage,
            "lab_unit": lab_unit,
            "current_unit_name": current_unit.ten_don_vi if current_unit else "",
        }
    )

@router.post("/create")
def create_unit(request: Request,
                ten_don_vi: str = Form(...),
                cap_do: int = Form(...),
                parent_id: str | None = Form(None),
                db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not _can_manage_units(user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ Admin, Trưởng khoa, Phó khoa hoặc Kỹ thuật viên trưởng được tạo đơn vị."
        )

    lab_unit = _get_lab_unit(db)
    if not lab_unit:
        raise HTTPException(status_code=400, detail="Chưa cấu hình đơn vị Khoa Xét nghiệm trong hệ thống.")

    parent = db.get(Units, parent_id) if parent_id else None

    if cap_do != 3:
        raise HTTPException(status_code=400, detail="Chỉ cho phép tạo Nhóm/Tổ trực thuộc Khoa Xét nghiệm.")

    if not parent:
        raise HTTPException(status_code=400, detail="Nhóm/Tổ bắt buộc phải chọn đơn vị cha là Khoa Xét nghiệm.")

    if getattr(parent, "id", None) != lab_unit.id:
        raise HTTPException(status_code=400, detail="Đơn vị cha chỉ được phép là Khoa Xét nghiệm.")

    if getattr(parent, "cap_do", None) != 2:
        raise HTTPException(status_code=400, detail="Đơn vị cha của Nhóm/Tổ phải là đơn vị cấp 2.")

    path = build_path(parent, ten_don_vi)
    u = Units(ten_don_vi=ten_don_vi, cap_do=cap_do, parent_id=parent_id, path=path)
    db.add(u); db.commit()
    return RedirectResponse(url="/units", status_code=302)

@router.post("/rename")
def rename_unit(request: Request,
                unit_id: str = Form(...),
                ten_don_vi_moi: str = Form(...),
                db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not _can_manage_units(user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ Admin, Trưởng khoa, Phó khoa hoặc Kỹ thuật viên trưởng được đổi tên."
        )

    u = db.get(Units, unit_id)
    if not u:
        return RedirectResponse(url="/units", status_code=302)

    ten_don_vi_moi = (ten_don_vi_moi or "").strip()
    if not ten_don_vi_moi:
        raise HTTPException(status_code=400, detail="Tên đơn vị mới không được để trống.")

    parent = db.get(Units, u.parent_id) if u.parent_id else None

    u.ten_don_vi = ten_don_vi_moi
    u.path = build_path(parent, ten_don_vi_moi)
    db.add(u)

    # Nếu đổi tên đơn vị cha thì toàn bộ path của con/cháu phải được cập nhật theo path mới của cha.
    # Nếu đổi tên đơn vị con thì chỉ cập nhật nhánh con của chính đơn vị đó, không ảnh hưởng path của cha.
    _rebuild_descendant_paths(db, u)

    db.commit()
    return RedirectResponse(url="/units", status_code=302)

@router.post("/retire")
def retire_unit(request: Request,
                unit_id: str = Form(...),
                reason: str = Form(""),
                db: Session = Depends(get_db)):
    user = login_required(request, db)
    if not _can_manage_units(user, db):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ Admin, Trưởng khoa, Phó khoa hoặc Kỹ thuật viên trưởng được thu hồi/đóng đơn vị."
        )

    u = db.get(Units, unit_id)
    if not u:
        return RedirectResponse(url="/units", status_code=302)

    reason = (reason or "").strip()

    # Chặn thu hồi đơn vị cha nếu còn đơn vị con đang ACTIVE
    if _has_active_children(db, u.id):
        raise HTTPException(
            status_code=400,
            detail="Không thể thu hồi đơn vị này vì còn đơn vị con đang hoạt động."
        )

    u.trang_thai = UnitStatus.RETIRED
    db.add(u)

    # Ghi chú:
    # - Thu hồi đơn vị con không ảnh hưởng đơn vị cha vì không sửa parent/path/trạng thái của cha.
    # - Hiện model Units chưa có cột lưu lý do thu hồi, nên chưa thể lưu reason vào DB
    #   nếu không thay đổi schema/model.
    # - reason hiện chỉ được nhận và kiểm tra, chưa persist.
    db.commit()
    return RedirectResponse(url="/units", status_code=302)
