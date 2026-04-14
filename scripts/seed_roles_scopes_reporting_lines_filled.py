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

DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

# ============================================================
# 1) DANH MỤC USER THEO SƠ ĐỒ ANH CHỐT
#    Dùng full_name vì anh đang cung cấp tên người, chưa có username thật
# ============================================================

PEOPLE = [
    {"full_name": "Nguyễn A",   "position": "HĐTV",                    "unit": "HĐTV"},
    {"full_name": "Nguyễn B",   "position": "BGĐ",                     "unit": "HĐTV"},
    {"full_name": "Nguyễn D",   "position": "Trưởng khoa",             "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn E",   "position": "Phó khoa",                "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị C", "position": "Kỹ thuật viên trưởng",    "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị F", "position": "Quản lý chất lượng",      "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn G",   "position": "Quản lý kỹ thuật",        "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn H",   "position": "Quản lý an toàn",         "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn K",   "position": "Quản lý vật tư",          "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị L", "position": "Quản lý trang thiết bị",  "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn M",   "position": "Quản lý môi trường",      "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn N",   "position": "Quản lý CNTT",            "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị O", "position": "Nhóm trưởng",             "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn P",   "position": "Nhóm phó",                "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn Q",   "position": "Nhân viên",               "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Trần Thị R", "position": "Nhóm trưởng",             "unit": "Nhóm Elisa"},
    {"full_name": "Nguyễn S",   "position": "Nhân viên",               "unit": "Nhóm Elisa"},
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

# ============================================================
# 2) TUYẾN BÁO CÁO ĐÚNG THEO SƠ ĐỒ
#    HĐTV là cấp trên của BGĐ dù cùng unit
#    Trưởng khoa là cấp trên của Phó khoa
# ============================================================

REPORTING_CONFIG = [
    ("Nguyễn B",   "Nguyễn A",   ReportingLineType.ADMINISTRATIVE, 1, "BGĐ báo cáo HĐTV"),
    ("Nguyễn D",   "Nguyễn B",   ReportingLineType.ADMINISTRATIVE, 1, "Trưởng khoa báo cáo BGĐ"),
    ("Nguyễn E",   "Nguyễn D",   ReportingLineType.ADMINISTRATIVE, 1, "Phó khoa báo cáo Trưởng khoa"),
    ("Trần Thị C", "Nguyễn D",   ReportingLineType.TECHNICAL,      1, "KTV trưởng báo cáo Trưởng khoa"),
    ("Trần Thị F", "Trần Thị C", ReportingLineType.QUALITY,        1, "QL chất lượng báo cáo KTV trưởng"),
    ("Nguyễn G",   "Trần Thị C", ReportingLineType.TECHNICAL,      1, "QL kỹ thuật báo cáo KTV trưởng"),
    ("Nguyễn H",   "Trần Thị C", ReportingLineType.SAFETY,         1, "QL an toàn báo cáo KTV trưởng"),
    ("Nguyễn K",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL vật tư báo cáo KTV trưởng"),
    ("Trần Thị L", "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL trang thiết bị báo cáo KTV trưởng"),
    ("Nguyễn M",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL môi trường báo cáo KTV trưởng"),
    ("Nguyễn N",   "Trần Thị C", ReportingLineType.OPERATIONS,     1, "QL CNTT báo cáo KTV trưởng"),
    ("Trần Thị O", "Trần Thị C", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm trưởng Sinh hóa - Miễn dịch báo cáo KTV trưởng"),
    ("Nguyễn P",   "Trần Thị O", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm phó báo cáo Nhóm trưởng"),
    ("Nguyễn Q",   "Trần Thị O", ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên báo cáo Nhóm trưởng"),
    ("Trần Thị R", "Trần Thị C", ReportingLineType.ADMINISTRATIVE, 1, "Nhóm trưởng Elisa báo cáo KTV trưởng"),
    ("Nguyễn S",   "Trần Thị R", ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên Elisa báo cáo Nhóm trưởng"),
]

# ============================================================
# 3) SCOPE QUẢN LÝ CHÉO THEO SƠ ĐỒ
#    Quản lý chức năng/công việc quản lý toàn bộ 8 nhóm theo mũi tên đỏ
# ============================================================

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
        print(f"[ROLE][OK] {role_code.value}")
        return role
    role = Roles(id=str(uuid.uuid4()), code=role_code, name=role_name)
    db.add(role)
    db.flush()
    print(f"[ROLE][CREATE] {role_code.value}")
    return role

def ensure_user_role(db, user: Users, role_code: RoleCode):
    role = db.query(Roles).filter(Roles.code == role_code).first()
    existed = db.query(UserRoles).filter(UserRoles.user_id == user.id, UserRoles.role_id == role.id).first()
    if existed:
        print(f"[USER_ROLE][OK] {user.full_name} -> {role_code.value}")
        return
    db.add(UserRoles(id=str(uuid.uuid4()), user_id=user.id, role_id=role.id))
    db.flush()
    print(f"[USER_ROLE][CREATE] {user.full_name} -> {role_code.value}")

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
        print(f"[MEMBER][OK] {user.full_name} -> {unit.ten_don_vi}")
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
    print(f"[MEMBER][CREATE] {user.full_name} -> {unit.ten_don_vi}")
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
        print(f"[SCOPE][OK] {manager.full_name} -> {target_unit.ten_don_vi}")
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
        print(f"[SCOPE][CREATE] {manager.full_name} -> {target_unit.ten_don_vi}")

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
        print(f"[LINE][OK] {from_user.full_name} -> {to_user.full_name}")
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
    print(f"[LINE][CREATE] {from_user.full_name} -> {to_user.full_name}")

def main():
    print(f"DB: {DB_PATH}")
    db = SessionLocal()
    try:
        print("\n=== SEED ROLES ===")
        for role_code, role_name in ROLE_SEED:
            ensure_role(db, role_code, role_name)

        print("\n=== SEED USER ROLES + MEMBERSHIPS ===")
        for item in PEOPLE:
            user = get_user_by_full_name(db, item["full_name"])
            if not user:
                print(f"[PEOPLE][SKIP] Chưa có user trong DB: {item['full_name']}")
                continue

            unit = get_unit_by_name(db, item["unit"])
            if not unit:
                print(f"[PEOPLE][SKIP] Không tìm thấy unit: {item['unit']}")
                continue

            role_code = POSITION_TO_ROLE[item["position"]]
            ensure_user_role(db, user, role_code)
            ensure_membership(db, user, unit, True)

            # Nếu là người của nhóm cấp 3 thì cho thêm membership phụ ở khoa
            if int(getattr(unit, "cap_do", 0) or 0) == 3 and getattr(unit, "parent_id", None):
                parent_unit = db.get(Units, unit.parent_id)
                if parent_unit:
                    ensure_membership(db, user, parent_unit, False)

        print("\n=== SEED MANAGEMENT SCOPES ===")
        for manager_name, manager_unit_name, target_names, scope_type, permissions, notes in SCOPE_CONFIG:
            manager = get_user_by_full_name(db, manager_name)
            if not manager:
                print(f"[SCOPE][SKIP] Chưa có user: {manager_name}")
                continue

            manager_unit = get_unit_by_name(db, manager_unit_name) if manager_unit_name else None
            for target_name in target_names:
                target_unit = get_unit_by_name(db, target_name)
                if not target_unit:
                    print(f"[SCOPE][SKIP] Không tìm thấy target unit: {target_name}")
                    continue
                ensure_management_scope(db, manager, manager_unit, target_unit, scope_type, permissions, notes)

        print("\n=== SEED REPORTING LINES ===")
        for from_name, to_name, line_type, priority_no, notes in REPORTING_CONFIG:
            from_user = get_user_by_full_name(db, from_name)
            to_user = get_user_by_full_name(db, to_name)
            if not from_user:
                print(f"[LINE][SKIP] Chưa có user: {from_name}")
                continue
            if not to_user:
                print(f"[LINE][SKIP] Chưa có user: {to_name}")
                continue
            ensure_reporting_line(db, from_user, to_user, line_type, priority_no, notes)

        db.commit()
        print("\n=== HOÀN THÀNH ===")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
