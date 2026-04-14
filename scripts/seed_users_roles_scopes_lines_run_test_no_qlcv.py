# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import (
    Users,
    Units,
    Roles,
    UserRoles,
    UserUnitMemberships,
    ManagementScopes,
    ManagementScopeType,
    ScopePermissions,
    PermissionCode,
    ReportingLines,
    ReportingLineType,
    RoleCode,
)
from app.security.crypto import hash_password

DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

DEFAULT_PASSWORD = "HvgL@2025"

PEOPLE = [
    {"full_name": "Nguyễn A",   "username": "nguyena",   "position": "HĐTV",                 "unit": "HĐTV"},
    {"full_name": "Nguyễn B",   "username": "nguyenb",   "position": "BGĐ",                  "unit": "HĐTV"},
    {"full_name": "Nguyễn D",   "username": "nguyend",   "position": "Trưởng khoa",          "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn E",   "username": "nguyene",   "position": "Phó khoa",             "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị C", "username": "tranthic",  "position": "Kỹ thuật viên trưởng", "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị F", "username": "tranthif",  "position": "Quản lý chất lượng",   "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn G",   "username": "nguyeng",   "position": "Quản lý kỹ thuật",     "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn H",   "username": "nguyenh",   "position": "Quản lý an toàn",      "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn K",   "username": "nguyenk",   "position": "Quản lý vật tư",       "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị L", "username": "tranthil",  "position": "Quản lý trang thiết bị","unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn M",   "username": "nguyenm",   "position": "Quản lý môi trường",   "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn N",   "username": "nguyenn",   "position": "Quản lý CNTT",         "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị O", "username": "tranthio",  "position": "Nhóm trưởng",          "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn P",   "username": "nguyenp",   "position": "Nhóm phó",             "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn Q",   "username": "nguyenq",   "position": "Nhân viên",            "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Trần Thị R", "username": "tranthir",  "position": "Nhóm trưởng",          "unit": "Nhóm Elisa"},
    {"full_name": "Nguyễn S",   "username": "nguyens",   "position": "Nhân viên",            "unit": "Nhóm Elisa"},
]

POSITION_TO_ROLE = {
    "HĐTV": RoleCode.ROLE_LANH_DAO,
    "BGĐ": RoleCode.ROLE_BGD,
    "Trưởng khoa": RoleCode.ROLE_TRUONG_KHOA,
    "Phó khoa": RoleCode.ROLE_PHO_TRUONG_KHOA,
    "Kỹ thuật viên trưởng": RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
    "Quản lý chất lượng": RoleCode.ROLE_QL_CHAT_LUONG,
    "Quản lý kỹ thuật": RoleCode.ROLE_QL_KY_THUAT,
    "Quản lý an toàn": RoleCode.ROLE_QL_AN_TOAN,
    "Quản lý vật tư": RoleCode.ROLE_QL_VAT_TU,
    "Quản lý trang thiết bị": RoleCode.ROLE_QL_TRANG_THIET_BI,
    "Quản lý môi trường": RoleCode.ROLE_QL_MOI_TRUONG,
    "Quản lý CNTT": RoleCode.ROLE_QL_CNTT,
    "Nhóm trưởng": RoleCode.ROLE_TRUONG_NHOM,
    "Nhóm phó": RoleCode.ROLE_PHO_NHOM,
    "Nhân viên": RoleCode.ROLE_NHAN_VIEN,
}

ROLE_SEED = [
    (RoleCode.ROLE_LANH_DAO, "HĐTV"),
    (RoleCode.ROLE_BGD, "BGĐ"),
    (RoleCode.ROLE_TRUONG_KHOA, "Trưởng khoa"),
    (RoleCode.ROLE_PHO_TRUONG_KHOA, "Phó khoa"),
    (RoleCode.ROLE_KY_THUAT_VIEN_TRUONG, "Kỹ thuật viên trưởng"),
    (RoleCode.ROLE_QL_CHAT_LUONG, "Quản lý chất lượng"),
    (RoleCode.ROLE_QL_KY_THUAT, "Quản lý kỹ thuật"),
    (RoleCode.ROLE_QL_AN_TOAN, "Quản lý an toàn"),
    (RoleCode.ROLE_QL_VAT_TU, "Quản lý vật tư"),
    (RoleCode.ROLE_QL_TRANG_THIET_BI, "Quản lý trang thiết bị"),
    (RoleCode.ROLE_QL_MOI_TRUONG, "Quản lý môi trường"),
    (RoleCode.ROLE_QL_CNTT, "Quản lý CNTT"),
    (RoleCode.ROLE_TRUONG_NHOM, "Nhóm/Tổ trưởng"),
    (RoleCode.ROLE_PHO_NHOM, "Nhóm/Tổ phó"),
    (RoleCode.ROLE_NHAN_VIEN, "Nhân viên"),
]

REPORTING_CONFIG = [
    ("Nguyễn B",   "Nguyễn A",   ReportingLineType.ADMINISTRATIVE, 1, "BGĐ báo cáo HĐTV"),
    ("Nguyễn D",   "Nguyễn B",   ReportingLineType.ADMINISTRATIVE, 1, "Trưởng khoa báo cáo BGĐ"),
    ("Nguyễn E",   "Nguyễn D",   ReportingLineType.ADMINISTRATIVE, 1, "Phó khoa báo cáo Trưởng khoa"),
    ("Trần Thị C", "Nguyễn D",   ReportingLineType.TECHNICAL,      1, "KTV trưởng dưới Trưởng khoa"),
    ("Trần Thị C", "Nguyễn E",   ReportingLineType.TECHNICAL,      2, "KTV trưởng dưới Phó khoa"),
    ("Trần Thị F", "Trần Thị C", ReportingLineType.QUALITY,        1, "QL chất lượng dưới KTV trưởng"),
    ("Nguyễn G",   "Trần Thị C", ReportingLineType.TECHNICAL,      1, "QL kỹ thuật dưới KTV trưởng"),
    ("Nguyễn H",   "Trần Thị C", ReportingLineType.SAFETY,         1, "QL an toàn dưới KTV trưởng"),
    ("Nguyễn K",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL vật tư dưới KTV trưởng"),
    ("Trần Thị L", "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL trang thiết bị dưới KTV trưởng"),
    ("Nguyễn M",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL môi trường dưới KTV trưởng"),
    ("Nguyễn N",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL CNTT dưới KTV trưởng"),
    ("Trần Thị O", "Trần Thị C", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm trưởng Sinh hóa - Miễn dịch dưới KTV trưởng"),
    ("Trần Thị O", "Trần Thị F", ReportingLineType.QUALITY,        2, "Nhóm trưởng Sinh hóa - Miễn dịch dưới QL chất lượng"),
    ("Trần Thị O", "Nguyễn G",   ReportingLineType.TECHNICAL,      3, "Nhóm trưởng Sinh hóa - Miễn dịch dưới QL kỹ thuật"),
    ("Nguyễn P",   "Trần Thị O", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm phó dưới Nhóm trưởng"),
    ("Nguyễn P",   "Trần Thị C", ReportingLineType.TECHNICAL,      2, "Nhóm phó dưới KTV trưởng"),
    ("Nguyễn P",   "Trần Thị F", ReportingLineType.QUALITY,        3, "Nhóm phó dưới QL chất lượng"),
    ("Nguyễn P",   "Nguyễn G",   ReportingLineType.TECHNICAL,      4, "Nhóm phó dưới QL kỹ thuật"),
    ("Nguyễn Q",   "Nguyễn P",   ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên dưới Nhóm phó"),
    ("Nguyễn Q",   "Trần Thị O", ReportingLineType.ADMINISTRATIVE, 2, "Nhân viên dưới Nhóm trưởng"),
    ("Trần Thị R", "Trần Thị C", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm trưởng Elisa dưới KTV trưởng"),
    ("Trần Thị R", "Trần Thị F", ReportingLineType.QUALITY,        2, "Nhóm trưởng Elisa dưới QL chất lượng"),
    ("Trần Thị R", "Nguyễn G",   ReportingLineType.TECHNICAL,      3, "Nhóm trưởng Elisa dưới QL kỹ thuật"),
    ("Nguyễn S",   "Trần Thị R", ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên Elisa dưới Nhóm trưởng"),
]

ALL_GROUPS = [
    "Nhóm Sinh hóa - Miễn dịch",
    "Nhóm Huyết học - Đông máu",
    "Nhóm Vi sinh",
    "Nhóm Sinh học phân tử",
    "Nhóm Elisa",
    "Nhóm Lấy máu - Gửi mẫu",
    "Nhóm Giải phẫu bệnh",
    "Nhóm Hỗ trợ sinh sản",
]

SCOPE_CONFIG = [
    ("Trần Thị F", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL chất lượng quản lý chéo toàn bộ các nhóm"),
    ("Nguyễn G", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL kỹ thuật quản lý chéo toàn bộ các nhóm"),
    ("Nguyễn H", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL an toàn quản lý chéo toàn bộ các nhóm"),
    ("Nguyễn K", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL vật tư quản lý chéo toàn bộ các nhóm"),
    ("Trần Thị L", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL trang thiết bị quản lý chéo toàn bộ các nhóm"),
    ("Nguyễn M", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL môi trường quản lý chéo toàn bộ các nhóm"),
    ("Nguyễn N", "Khoa Xét nghiệm", ALL_GROUPS, ManagementScopeType.FULL_UNIT,
     [PermissionCode.VIEW_WORK, PermissionCode.ASSIGN_WORK, PermissionCode.VIEW_PLANS, PermissionCode.VIEW_FILES, PermissionCode.REQUEST_COORDINATION],
     "QL CNTT quản lý chéo toàn bộ các nhóm"),
]

def get_user_by_full_name(db, full_name: str):
    return db.query(Users).filter(Users.full_name == full_name).first()

def get_unit_by_name(db, unit_name: str):
    return db.query(Units).filter(Units.ten_don_vi == unit_name).first()

def ensure_role(db, role_code: RoleCode, role_name: str):
    role = db.query(Roles).filter(Roles.code == role_code).first()
    if role:
        if role.name != role_name:
            role.name = role_name
            db.add(role)
            db.flush()
        return role
    role = Roles(id=str(uuid.uuid4()), code=role_code, name=role_name)
    db.add(role)
    db.flush()
    return role

def ensure_user(db, full_name: str, username: str):
    row = db.query(Users).filter(Users.username == username).first()
    if row:
        if row.full_name != full_name:
            row.full_name = full_name
            db.add(row)
            db.flush()
        return row
    row = Users(
        id=str(uuid.uuid4()),
        full_name=full_name,
        username=username,
        email=None,
        phone=None,
        password_hash=hash_password(DEFAULT_PASSWORD),
        status="ACTIVE",
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.flush()
    return row

def ensure_user_role(db, user: Users, role_code: RoleCode):
    role = db.query(Roles).filter(Roles.code == role_code).first()
    existed = db.query(UserRoles).filter(UserRoles.user_id == user.id, UserRoles.role_id == role.id).first()
    if existed:
        return
    db.add(UserRoles(id=str(uuid.uuid4()), user_id=user.id, role_id=role.id))
    db.flush()

def ensure_membership(db, user: Users, unit: Units, is_primary: bool):
    existed = db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.unit_id == unit.id
    ).first()
    if existed:
        if existed.is_primary != is_primary:
            existed.is_primary = is_primary
            db.add(existed)
            db.flush()
        return existed

    row = UserUnitMemberships(
        id=str(uuid.uuid4()),
        user_id=user.id,
        unit_id=unit.id,
        is_primary=is_primary,
        is_active=True,
    )
    db.add(row)
    db.flush()
    return row

def ensure_scope_permission(db, scope_id: str, permission_code: PermissionCode):
    existed = db.query(ScopePermissions).filter(
        ScopePermissions.scope_id == scope_id,
        ScopePermissions.permission_code == permission_code
    ).first()
    if existed:
        return
    db.add(ScopePermissions(
        id=str(uuid.uuid4()),
        scope_id=scope_id,
        permission_code=permission_code,
    ))
    db.flush()

def ensure_management_scope(db, manager: Users, manager_unit: Units | None, target_unit: Units, scope_type, permissions, notes):
    existed = db.query(ManagementScopes).filter(
        ManagementScopes.manager_user_id == manager.id,
        ManagementScopes.manager_unit_id == (manager_unit.id if manager_unit else None),
        ManagementScopes.target_unit_id == target_unit.id,
        ManagementScopes.scope_type == scope_type,
    ).first()
    if existed:
        scope = existed
    else:
        scope = ManagementScopes(
            id=str(uuid.uuid4()),
            manager_user_id=manager.id,
            manager_unit_id=manager_unit.id if manager_unit else None,
            target_unit_id=target_unit.id,
            scope_type=scope_type,
            is_active=True,
            notes=notes,
            created_at=datetime.utcnow(),
        )
        db.add(scope)
        db.flush()

    for perm in permissions:
        ensure_scope_permission(db, scope.id, perm)

def ensure_reporting_line(db, from_user: Users, to_user: Users, line_type, priority_no, notes):
    existed = db.query(ReportingLines).filter(
        ReportingLines.from_user_id == from_user.id,
        ReportingLines.to_user_id == to_user.id,
        ReportingLines.line_type == line_type,
        ReportingLines.priority_no == priority_no,
    ).first()
    if existed:
        return
    db.add(ReportingLines(
        id=str(uuid.uuid4()),
        from_user_id=from_user.id,
        to_user_id=to_user.id,
        line_type=line_type,
        priority_no=priority_no,
        is_active=True,
        notes=notes,
        created_at=datetime.utcnow(),
    ))
    db.flush()

def main():
    print(f"DB: {DB_PATH}")
    db = SessionLocal()
    try:
        for role_code, role_name in ROLE_SEED:
            ensure_role(db, role_code, role_name)

        for item in PEOPLE:
            unit = get_unit_by_name(db, item["unit"])
            if not unit:
                print(f"[SKIP][UNIT] {item['full_name']} - {item['unit']}")
                continue

            user = ensure_user(db, item["full_name"], item["username"])
            ensure_user_role(db, user, POSITION_TO_ROLE[item["position"]])
            ensure_membership(db, user, unit, True)

            if int(getattr(unit, "cap_do", 0) or 0) == 3 and getattr(unit, "parent_id", None):
                parent_unit = db.get(Units, unit.parent_id)
                if parent_unit:
                    ensure_membership(db, user, parent_unit, False)

        for manager_name, manager_unit_name, target_names, scope_type, permissions, notes in SCOPE_CONFIG:
            manager = get_user_by_full_name(db, manager_name)
            if not manager:
                continue
            manager_unit = get_unit_by_name(db, manager_unit_name) if manager_unit_name else None
            for target_name in target_names:
                target_unit = get_unit_by_name(db, target_name)
                if not target_unit:
                    continue
                ensure_management_scope(db, manager, manager_unit, target_unit, scope_type, permissions, notes)

        for from_name, to_name, line_type, priority_no, notes in REPORTING_CONFIG:
            from_user = get_user_by_full_name(db, from_name)
            to_user = get_user_by_full_name(db, to_name)
            if not from_user or not to_user:
                continue
            ensure_reporting_line(db, from_user, to_user, line_type, priority_no, notes)

        db.commit()
        print("=== HOÀN THÀNH ===")
        print(f"Password mặc định cho user seed mới: {DEFAULT_PASSWORD}")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
