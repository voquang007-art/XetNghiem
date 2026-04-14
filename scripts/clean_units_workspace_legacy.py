# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import shutil
from datetime import datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import func
from app.database import SessionLocal
from app.models import (
    Units, UnitStatus,
    UserUnitMemberships, Files, Plans, PlanItems, Tasks,
    DocumentDrafts, DocumentDraftActions,
    VisibilityGrants, ManagementScopes,
)

# =========================
# CẤU HÌNH
# =========================

# Chế độ:
# - REPORT_ONLY = chỉ rà, không sửa DB
# - RETIRE_MATCHED = đánh dấu RETIRED các đơn vị khớp danh sách/keyword
MODE = "REPORT_ONLY"   # đổi thành "RETIRE_MATCHED" khi anh chốt

# Khớp theo tên đơn vị cụ thể (khuyên dùng)
TARGET_UNIT_NAMES = [
    # Ví dụ:
    # "Hội đồng thành viên",
    # "Phòng Hành chính",
    # "Tổ ABC",
]

# Khớp mờ theo từ khóa tên đơn vị (dùng cẩn thận)
TARGET_NAME_KEYWORDS = [
    # Ví dụ:
    # "workspace",
    # "phòng",
    # "tổ",
]

# Nếu True thì chỉ xử lý đơn vị ACTIVE
ONLY_ACTIVE = True

# Nếu True: không RETIRE đơn vị đang còn tham chiếu nghiệp vụ
SKIP_IF_HAS_REFERENCES = True


def backup_db_file() -> None:
    db_path = os.path.join(PROJECT_ROOT, "instance", "qlcv.sqlite3")
    if not os.path.exists(db_path):
        db_path = os.path.join(PROJECT_ROOT, "app.db")

    if not os.path.exists(db_path):
        print("[WARN] Không tìm thấy file DB để backup tự động.")
        return

    backup_dir = os.path.join(PROJECT_ROOT, "backup")
    os.makedirs(backup_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = os.path.join(backup_dir, f"{os.path.basename(db_path)}.{stamp}.bak")
    shutil.copy2(db_path, dst)
    print(f"[OK] Đã backup DB: {dst}")


def normalize(s: str) -> str:
    return (s or "").strip().lower()


def unit_matches(name: str) -> bool:
    n = normalize(name)

    exact_names = {normalize(x) for x in TARGET_UNIT_NAMES if normalize(x)}
    if n in exact_names:
        return True

    keywords = [normalize(x) for x in TARGET_NAME_KEYWORDS if normalize(x)]
    for kw in keywords:
        if kw and kw in n:
            return True

    return False


def ref_counts(db, unit_id: str) -> dict:
    data = {
        "memberships": db.query(func.count(UserUnitMemberships.id)).filter(UserUnitMemberships.unit_id == unit_id).scalar() or 0,
        "files": db.query(func.count(Files.id)).filter(Files.unit_id == unit_id).scalar() or 0,
        "plans": db.query(func.count(Plans.id)).filter(Plans.unit_id == unit_id).scalar() or 0,
        "plan_items_assignee": db.query(func.count(PlanItems.id)).filter(PlanItems.assignee_unit_id == unit_id).scalar() or 0,
        "tasks_unit": db.query(func.count(Tasks.id)).filter(Tasks.unit_id == unit_id).scalar() or 0,
        "tasks_assignee": db.query(func.count(Tasks.id)).filter(Tasks.assigned_to_unit_id == unit_id).scalar() or 0,
        "draft_created": db.query(func.count(DocumentDrafts.id)).filter(DocumentDrafts.created_unit_id == unit_id).scalar() or 0,
        "draft_handler": db.query(func.count(DocumentDrafts.id)).filter(DocumentDrafts.current_handler_unit_id == unit_id).scalar() or 0,
        "draft_actions_from": db.query(func.count(DocumentDraftActions.id)).filter(DocumentDraftActions.from_unit_id == unit_id).scalar() or 0,
        "draft_actions_to": db.query(func.count(DocumentDraftActions.id)).filter(DocumentDraftActions.to_unit_id == unit_id).scalar() or 0,
        "visibility_grants": db.query(func.count(VisibilityGrants.id)).filter(VisibilityGrants.grantee_unit_id == unit_id).scalar() or 0,
        "management_scope_manager_unit": db.query(func.count(ManagementScopes.id)).filter(ManagementScopes.manager_unit_id == unit_id).scalar() or 0,
        "management_scope_target_unit": db.query(func.count(ManagementScopes.id)).filter(ManagementScopes.target_unit_id == unit_id).scalar() or 0,
        "children": db.query(func.count(Units.id)).filter(Units.parent_id == unit_id, Units.trang_thai == UnitStatus.ACTIVE).scalar() or 0,
    }
    data["total_refs"] = sum(v for k, v in data.items() if k != "children") + data["children"]
    return data


def print_unit_report(db):
    rows = db.query(Units).order_by(Units.cap_do.asc(), Units.ten_don_vi.asc()).all()
    print("=" * 120)
    print("DANH SÁCH ĐƠN VỊ")
    print("=" * 120)
    for u in rows:
        refs = ref_counts(db, u.id)
        print(
            f"[{u.cap_do}] {u.ten_don_vi} | id={u.id} | parent_id={u.parent_id} | "
            f"status={getattr(u.trang_thai, 'value', u.trang_thai)} | refs={refs['total_refs']}"
        )
    print("=" * 120)


def main():
    db = SessionLocal()
    try:
        backup_db_file()
        print_unit_report(db)

        matched = []
        rows = db.query(Units).order_by(Units.cap_do.asc(), Units.ten_don_vi.asc()).all()
        for u in rows:
            status_val = getattr(u.trang_thai, "value", u.trang_thai)
            if ONLY_ACTIVE and status_val != UnitStatus.ACTIVE.value:
                continue
            if unit_matches(u.ten_don_vi):
                matched.append(u)

        print("\nĐƠN VỊ KHỚP TIÊU CHÍ:")
        for u in matched:
            refs = ref_counts(db, u.id)
            print(
                f"- [{u.cap_do}] {u.ten_don_vi} | id={u.id} | "
                f"status={getattr(u.trang_thai, 'value', u.trang_thai)} | refs={refs}"
            )

        if MODE != "RETIRE_MATCHED":
            print("\n[REPORT_ONLY] Không thay đổi dữ liệu.")
            return

        changed = 0
        for u in matched:
            refs = ref_counts(db, u.id)

            if SKIP_IF_HAS_REFERENCES and refs["total_refs"] > 0:
                print(f"[SKIP] {u.ten_don_vi} còn tham chiếu, không RETIRE.")
                continue

            u.trang_thai = UnitStatus.RETIRED
            db.add(u)
            changed += 1
            print(f"[RETIRE] {u.ten_don_vi}")

        db.commit()
        print(f"\n[OK] Đã cập nhật {changed} đơn vị sang RETIRED.")
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()