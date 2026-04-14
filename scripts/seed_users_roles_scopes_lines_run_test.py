
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import uuid
import re
import unicodedata
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import (
    Users, UserStatus, Units, Roles, UserRoles, UserUnitMemberships,
    ManagementScopes, ManagementScopeType, ScopePermissions, PermissionCode,
    ReportingLines, ReportingLineType, RoleCode
)
from app.security.crypto import hash_password

DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)

DEFAULT_PASSWORD = "HvgL@2025"

PEOPLE = [
    {"full_name": "Nguyễn A",   "position": "HĐTV",                 "unit": "HĐTV"},
    {"full_name": "Nguyễn B",   "position": "BGĐ",                  "unit": "HĐTV"},
    {"full_name": "Nguyễn D",   "position": "Trưởng khoa",          "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn E",   "position": "Phó khoa",             "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị C", "position": "Kỹ thuật viên trưởng", "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị F", "position": "Quản lý chất lượng",   "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn G",   "position": "Quản lý kỹ thuật",     "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn H",   "position": "Quản lý an toàn",      "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn K",   "position": "Quản lý vật tư",       "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị L", "position": "Quản lý trang thiết bị","unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn M",   "position": "Quản lý môi trường",   "unit": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn N",   "position": "Quản lý CNTT",         "unit": "Khoa Xét nghiệm"},
    {"full_name": "QLCV Test",  "position": "Quản lý công việc",    "unit": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị O", "position": "Nhóm trưởng",          "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn P",   "position": "Nhóm phó",             "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn Q",   "position": "Nhân viên",            "unit": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Trần Thị R", "position": "Nhóm trưởng",          "unit": "Nhóm Elisa"},
    {"full_name": "Nguyễn S",   "position": "Nhân viên",            "unit": "Nhóm Elisa"},
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
    "Quản lý công việc": RoleCode.ROLE_QL_CONG_VIEC,
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
    (RoleCode.ROLE_QL_CONG_VIEC, "Quản lý công việc"),
    (RoleCode.ROLE_TRUONG_NHOM, "Nhóm/Tổ trưởng"),
    (RoleCode.ROLE_PHO_NHOM, "Nhóm/Tổ phó"),
    (RoleCode.ROLE_NHAN_VIEN, "Nhân viên"),
    (RoleCode.ROLE_TRUONG_PHONG, "Tương thích ngược - Trưởng phòng"),
    (RoleCode.ROLE_PHO_PHONG, "Tương thích ngược - Phó phòng"),
    (RoleCode.ROLE_TO_TRUONG, "Tương thích ngược - Tổ trưởng"),
    (RoleCode.ROLE_PHO_TO, "Tương thích ngược - Tổ phó"),
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

FUNCTIONAL_MANAGERS = [
    "Trần Thị F",
    "Nguyễn G",
    "Nguyễn H",
    "Nguyễn K",
    "Trần Thị L",
    "Nguyễn M",
    "Nguyễn N",
]

REPORTING_CONFIG = [
    ("Nguyễn B",   "Nguyễn A",   ReportingLineType.ADMINISTRATIVE, 1, "BGĐ báo cáo HĐTV"),
    ("Nguyễn D",   "Nguyễn B",   ReportingLineType.ADMINISTRATIVE, 1, "Trưởng khoa báo cáo BGĐ"),
    ("Nguyễn E",   "Nguyễn D",   ReportingLineType.ADMINISTRATIVE, 1, "Phó khoa báo cáo Trưởng khoa"),
    ("Trần Thị C", "Nguyễn D",   ReportingLineType.TECHNICAL,      1, "KTV trưởng báo cáo Trưởng khoa"),
    ("Trần Thị C", "Nguyễn E",   ReportingLineType.TECHNICAL,      2, "KTV trưởng báo cáo Phó khoa"),
]

def slugify_name(text: str) -> str:
    s = unicodedata.normalize("NFD", text or "")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace("đ", "d").replace("Đ", "D").lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or f"user_{uuid.uuid4().hex[:6]}"

def get_unit_by_name(db, name: str):
    return db.query(Units).filter(Units.ten_don_vi == name).first()

def ensure_role(db, code: RoleCode, name: str):
    role = db.query(Roles).filter(Roles.code == code).first()
    if role:
        if role.name != name:
            role.name = name
            db.add(role)
            db.flush()
        print(f"[ROLE][OK] {code.value}")
        return role
    role = Roles(id=str(uuid.uuid4()), code=code, name=name)
    db.add(role)
    db.flush()
    print(f"[ROLE][CREATE] {code.value}")
    return role

def ensure_user(db, full_name: str):
    user = db.query(Users).filter(Users.full_name == full_name).first()
    if user:
        if user.status != UserStatus.ACTIVE:
            user.status = UserStatus.ACTIVE
            db.add(user)
            db.flush()
        print(f"[USER][OK] {full_name} | username={user.username}")
        return user

    base = slugify_name(full_name)
    username = base
    idx = 1
    while db.query(Users).filter(Users.username == username).first():
        idx += 1
        username = f"{base}_{idx}"

    user = Users(
        id=str(uuid.uuid4()),
        username=username,
        full_name=full_name,
        password_hash=hash_password(DEFAULT_PASSWORD),
        status=UserStatus.ACTIVE,
        created_at=datetime.utcnow(),
    )
    db.add(user)
    db.flush()
    print(f"[USER][CREATE] {full_name} | username={username} | password={DEFAULT_PASSWORD}")
    return user

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
    row = db.query(UserUnitMemberships).filter(
        UserUnitMemberships.user_id == user.id,
        UserUnitMemberships.unit_id == unit.id
    ).first()
    if row:
        if row.is_primary != is_primary:
            row.is_primary = is_primary
            db.add(row)
            db.flush()
        print(f"[MEMBER][OK] {user.full_name} -> {unit.ten_don_vi}")
        return
    db.add(UserUnitMemberships(
        id=str(uuid.uuid4()),
        user_id=user.id,
        unit_id=unit.id,
        is_primary=is_primary,
        is_active=True,
        start_date=datetime.utcnow(),
    ))
    db.flush()
    print(f"[MEMBER][CREATE] {user.full_name} -> {unit.ten_don_vi}")

def ensure_scope_permission(db, scope_id: str, code: PermissionCode):
    existed = db.query(ScopePermissions).filter(
        ScopePermissions.scope_id == scope_id,
        ScopePermissions.permission_code == code
    ).first()
    if not existed:
        db.add(ScopePermissions(id=str(uuid.uuid4()), scope_id=scope_id, permission_code=code))
        db.flush()

def ensure_management_scope(db, manager: Users, manager_unit: Units | None, target_unit: Units, scope_type, permissions, notes):
    scope = db.query(ManagementScopes).filter(
        ManagementScopes.manager_user_id == manager.id,
        ManagementScopes.manager_unit_id == (manager_unit.id if manager_unit else None),
        ManagementScopes.target_unit_id == target_unit.id,
        ManagementScopes.scope_type == scope_type,
    ).first()
    if not scope:
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
    else:
        print(f"[SCOPE][OK] {manager.full_name} -> {target_unit.ten_don_vi}")

    for perm in permissions:
        ensure_scope_permission(db, scope.id, perm)

def ensure_reporting_line(db, from_user: Users, to_user: Users, line_type, priority_no: int, notes: str):
    existed = db.query(ReportingLines).filter(
        ReportingLines.from_user_id == from_user.id,
        ReportingLines.to_user_id == to_user.id,
        ReportingLines.line_type == line_type,
        ReportingLines.priority_no == priority_no,
    ).first()
    if existed:
        print(f"[LINE][OK] {from_user.full_name} -> {to_user.full_name} ({priority_no})")
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
    print(f"[LINE][CREATE] {from_user.full_name} -> {to_user.full_name} ({priority_no})")

def main():
    print(f"DB: {DB_PATH}")
    db = SessionLocal()
    try:
        print("\n=== SEED ROLES ===")
        for code, name in ROLE_SEED:
            ensure_role(db, code, name)

        print("\n=== SEED USERS + USER ROLES + MEMBERSHIPS ===")
        people_users = {}
        for item in PEOPLE:
            user = ensure_user(db, item["full_name"])
            people_users[item["full_name"]] = user
            unit = get_unit_by_name(db, item["unit"])
            if not unit:
                print(f"[PEOPLE][SKIP] Không tìm thấy unit: {item['unit']}")
                continue
            ensure_user_role(db, user, POSITION_TO_ROLE[item["position"]])
            ensure_membership(db, user, unit, True)
            if int(getattr(unit, "cap_do", 0) or 0) == 3 and getattr(unit, "parent_id", None):
                parent = db.get(Units, unit.parent_id)
                if parent:
                    ensure_membership(db, user, parent, False)

        print("\n=== SEED MANAGEMENT SCOPES ===")
        manager_unit = get_unit_by_name(db, "Khoa Xét nghiệm")
        perms = [
            PermissionCode.VIEW_WORK,
            PermissionCode.ASSIGN_WORK,
            PermissionCode.VIEW_PLANS,
            PermissionCode.VIEW_FILES,
            PermissionCode.REQUEST_COORDINATION,
        ]
        for manager_name in FUNCTIONAL_MANAGERS + ["QLCV Test", "Trần Thị C"]:
            manager = people_users.get(manager_name) or db.query(Users).filter(Users.full_name == manager_name).first()
            if not manager:
                print(f"[SCOPE][SKIP] Chưa có user: {manager_name}")
                continue
            for group_name in ALL_GROUPS:
                target = get_unit_by_name(db, group_name)
                if not target:
                    print(f"[SCOPE][SKIP] Không tìm thấy unit: {group_name}")
                    continue
                ensure_management_scope(
                    db,
                    manager=manager,
                    manager_unit=manager_unit,
                    target_unit=target,
                    scope_type=ManagementScopeType.FULL_UNIT,
                    permissions=perms,
                    notes=f"{manager_name} quản lý thử nghiệm theo sơ đồ",
                )

        print("\n=== SEED REPORTING LINES ===")
        # base reporting
        for from_name, to_name, line_type, priority_no, notes in REPORTING_CONFIG:
            f = people_users.get(from_name)
            t = people_users.get(to_name)
            if f and t:
                ensure_reporting_line(db, f, t, line_type, priority_no, notes)

        # Functional managers -> KTV trưởng
        ktv = people_users.get("Trần Thị C")
        qlcv = people_users.get("QLCV Test")
        for mgr_name in FUNCTIONAL_MANAGERS:
            mgr = people_users.get(mgr_name)
            if mgr and ktv:
                ensure_reporting_line(db, mgr, ktv, ReportingLineType.FUNCTIONAL, 1, f"{mgr_name} dưới KTV trưởng")

        # QLCV dưới KTV trưởng và dưới các quản lý chức năng
        if qlcv and ktv:
            ensure_reporting_line(db, qlcv, ktv, ReportingLineType.OPERATIONS, 1, "QLCV dưới KTV trưởng")
        if qlcv:
            pr = 2
            for mgr_name in FUNCTIONAL_MANAGERS:
                mgr = people_users.get(mgr_name)
                if mgr:
                    ensure_reporting_line(db, qlcv, mgr, ReportingLineType.OPERATIONS, pr, "QLCV dưới quản lý chức năng")
                    pr += 1

        # Nhóm trưởng dưới KTV trưởng, QLCV, quản lý chức năng
        for leader_name in ["Trần Thị O", "Trần Thị R"]:
            leader = people_users.get(leader_name)
            if not leader:
                continue
            if ktv:
                ensure_reporting_line(db, leader, ktv, ReportingLineType.ADMINISTRATIVE, 1, "Nhóm trưởng dưới KTV trưởng")
            if qlcv:
                ensure_reporting_line(db, leader, qlcv, ReportingLineType.OPERATIONS, 2, "Nhóm trưởng dưới QLCV")
            pr = 3
            for mgr_name in FUNCTIONAL_MANAGERS:
                mgr = people_users.get(mgr_name)
                if mgr:
                    ensure_reporting_line(db, leader, mgr, ReportingLineType.FUNCTIONAL, pr, "Nhóm trưởng dưới quản lý chức năng")
                    pr += 1

        # Nhóm phó dưới nhóm trưởng, KTV trưởng, QLCV, quản lý chức năng
        pho = people_users.get("Nguyễn P")
        leader_hoa = people_users.get("Trần Thị O")
        if pho:
            if leader_hoa:
                ensure_reporting_line(db, pho, leader_hoa, ReportingLineType.ADMINISTRATIVE, 1, "Nhóm phó dưới Nhóm trưởng")
            if ktv:
                ensure_reporting_line(db, pho, ktv, ReportingLineType.ADMINISTRATIVE, 2, "Nhóm phó dưới KTV trưởng")
            if qlcv:
                ensure_reporting_line(db, pho, qlcv, ReportingLineType.OPERATIONS, 3, "Nhóm phó dưới QLCV")
            pr = 4
            for mgr_name in FUNCTIONAL_MANAGERS:
                mgr = people_users.get(mgr_name)
                if mgr:
                    ensure_reporting_line(db, pho, mgr, ReportingLineType.FUNCTIONAL, pr, "Nhóm phó dưới quản lý chức năng")
                    pr += 1

        # Nhân viên dưới Nhóm phó và Nhóm trưởng
        nv_q = people_users.get("Nguyễn Q")
        if nv_q:
            if pho:
                ensure_reporting_line(db, nv_q, pho, ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên dưới Nhóm phó")
            if leader_hoa:
                ensure_reporting_line(db, nv_q, leader_hoa, ReportingLineType.ADMINISTRATIVE, 2, "Nhân viên dưới Nhóm trưởng")

        nv_s = people_users.get("Nguyễn S")
        leader_elisa = people_users.get("Trần Thị R")
        if nv_s and leader_elisa:
            ensure_reporting_line(db, nv_s, leader_elisa, ReportingLineType.ADMINISTRATIVE, 1, "Nhân viên Elisa dưới Nhóm trưởng")

        db.commit()
        print("\n=== HOÀN THÀNH ===")
        print(f"Password mặc định cho các user seed mới: {DEFAULT_PASSWORD}")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    main()
