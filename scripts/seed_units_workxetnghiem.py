# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import uuid

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Units, UnitStatus, UnitType

DB_PATH = os.path.join(PROJECT_ROOT, "instance", "workxetnghiem.sqlite3")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=ENGINE)


def slugify(text: str) -> str:
    import re
    s = (text or "").strip().lower()
    repl = {
        "à":"a","á":"a","ạ":"a","ả":"a","ã":"a",
        "â":"a","ầ":"a","ấ":"a","ậ":"a","ẩ":"a","ẫ":"a",
        "ă":"a","ằ":"a","ắ":"a","ặ":"a","ẳ":"a","ẵ":"a",
        "è":"e","é":"e","ẹ":"e","ẻ":"e","ẽ":"e",
        "ê":"e","ề":"e","ế":"e","ệ":"e","ể":"e","ễ":"e",
        "ì":"i","í":"i","ị":"i","ỉ":"i","ĩ":"i",
        "ò":"o","ó":"o","ọ":"o","ỏ":"o","õ":"o",
        "ô":"o","ồ":"o","ố":"o","ộ":"o","ổ":"o","ỗ":"o",
        "ơ":"o","ờ":"o","ớ":"o","ợ":"o","ở":"o","ỡ":"o",
        "ù":"u","ú":"u","ụ":"u","ủ":"u","ũ":"u",
        "ư":"u","ừ":"u","ứ":"u","ự":"u","ử":"u","ữ":"u",
        "ỳ":"y","ý":"y","ỵ":"y","ỷ":"y","ỹ":"y",
        "đ":"d",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def build_path(parent: Units | None, name: str) -> str:
    base = "/org"
    if parent:
        return f"{parent.path}/{slugify(name)}"
    return f"{base}/{slugify(name)}"


def ensure_unit(db, ten_don_vi: str, cap_do: int, parent: Units | None = None, unit_type: UnitType = UnitType.GENERAL, order_index: int = 0) -> Units:
    q = db.query(Units).filter(Units.ten_don_vi == ten_don_vi, Units.cap_do == cap_do)
    if parent is None:
        q = q.filter(Units.parent_id.is_(None))
    else:
        q = q.filter(Units.parent_id == parent.id)

    unit = q.first()
    if not unit:
        unit = Units(
            id=str(uuid.uuid4()),
            ten_don_vi=ten_don_vi,
            cap_do=cap_do,
            level_no=cap_do,
            parent_id=parent.id if parent else None,
            unit_type=unit_type,
            ma_don_vi=None,
            path=build_path(parent, ten_don_vi),
            trang_thai=UnitStatus.ACTIVE,
            order_index=order_index,
        )
        db.add(unit)
        db.flush()
        print(f"[CREATE] [{cap_do}] {ten_don_vi}")
    else:
        unit.level_no = cap_do
        unit.parent_id = parent.id if parent else None
        unit.unit_type = unit_type
        unit.path = build_path(parent, ten_don_vi)
        unit.trang_thai = UnitStatus.ACTIVE
        unit.order_index = order_index
        db.add(unit)
        db.flush()
        print(f"[UPDATE] [{cap_do}] {ten_don_vi}")
    return unit


def canonical_paths() -> set[str]:
    return {
        "/org/hdtv",
        "/org/hdtv/khoa-xet-nghiem",
        "/org/hdtv/khoa-xet-nghiem/nhom-sinh-hoa-mien-dich",
        "/org/hdtv/khoa-xet-nghiem/nhom-huyet-hoc-dong-mau",
        "/org/hdtv/khoa-xet-nghiem/nhom-vi-sinh",
        "/org/hdtv/khoa-xet-nghiem/nhom-sinh-hoc-phan-tu",
        "/org/hdtv/khoa-xet-nghiem/nhom-elisa",
        "/org/hdtv/khoa-xet-nghiem/nhom-lay-mau-gui-mau",
        "/org/hdtv/khoa-xet-nghiem/nhom-giai-phau-benh",
        "/org/hdtv/khoa-xet-nghiem/nhom-ho-tro-sinh-san",
    }


def retire_non_canonical_active_units(db) -> int:
    keep = canonical_paths()
    rows = (
        db.query(Units)
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )

    retired = 0
    for u in rows:
        current_path = (u.path or "").strip()
        if current_path not in keep:
            u.trang_thai = UnitStatus.RETIRED
            db.add(u)
            retired += 1
            print(f"[RETIRE] [{u.cap_do}] {u.ten_don_vi} | path={u.path}")
    return retired


def print_active_units(db):
    rows = (
        db.query(Units)
        .filter(Units.trang_thai == UnitStatus.ACTIVE)
        .order_by(Units.cap_do.asc(), Units.order_index.asc(), Units.ten_don_vi.asc())
        .all()
    )
    print("\n=== ACTIVE UNITS SAU KHI SEED ===")
    for u in rows:
        print(f"[{u.cap_do}] {u.ten_don_vi} | parent_id={u.parent_id} | path={u.path}")


def main():
    if not os.path.exists(os.path.dirname(DB_PATH)):
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    db = SessionLocal()
    try:
        print(f"DB: {DB_PATH}")
        hdtv = ensure_unit(db, "HĐTV", 1, None, UnitType.LEADERSHIP, 1)
        khoa = ensure_unit(db, "Khoa Xét nghiệm", 2, hdtv, UnitType.LAB_LEADERSHIP, 10)
        ensure_unit(db, "Nhóm Sinh hóa - Miễn dịch", 3, khoa, UnitType.EXECUTION_GROUP, 101)
        ensure_unit(db, "Nhóm Huyết học - Đông máu", 3, khoa, UnitType.EXECUTION_GROUP, 102)
        ensure_unit(db, "Nhóm Vi sinh", 3, khoa, UnitType.EXECUTION_GROUP, 103)
        ensure_unit(db, "Nhóm Sinh học phân tử", 3, khoa, UnitType.EXECUTION_GROUP, 104)
        ensure_unit(db, "Nhóm Elisa", 3, khoa, UnitType.EXECUTION_GROUP, 105)
        ensure_unit(db, "Nhóm Lấy máu - Gửi mẫu", 3, khoa, UnitType.EXECUTION_GROUP, 106)
        ensure_unit(db, "Nhóm Giải phẫu bệnh", 3, khoa, UnitType.EXECUTION_GROUP, 107)
        ensure_unit(db, "Nhóm Hỗ trợ sinh sản", 3, khoa, UnitType.EXECUTION_GROUP, 108)

        retired = retire_non_canonical_active_units(db)
        db.commit()
        print(f"\n=== SEED UNITS THÀNH CÔNG | RETIRED: {retired} ===")
        print_active_units(db)
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
