import os
import sys
import sqlite3
from pathlib import Path


def find_db_path() -> Path:
    """
    Ưu tiên:
    1) tham số dòng lệnh
    2) ./instance/*.sqlite3 hoặc *.db
    """
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Không tìm thấy DB: {p}")
        return p

    base_dir = Path.cwd()
    instance_dir = base_dir / "instance"
    if not instance_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục instance: {instance_dir}")

    candidates = []
    for pattern in ("*.sqlite3", "*.db", "*.sqlite"):
        candidates.extend(instance_dir.glob(pattern))

    if not candidates:
        raise FileNotFoundError(f"Không tìm thấy file DB trong: {instance_dir}")

    # Ưu tiên tên có vẻ đúng nhất
    preferred = []
    for p in candidates:
        name = p.name.lower()
        score = 0
        if "workxetnghiem" in name:
            score += 100
        if "sqlite3" in name:
            score += 10
        preferred.append((score, p))

    preferred.sort(key=lambda x: (-x[0], x[1].name))
    return preferred[0][1].resolve()


def q_all(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def print_section(title: str):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def print_rows(rows, headers=None):
    if headers:
        print(" | ".join(headers))
        print("-" * 100)
    for row in rows:
        print(" | ".join("" if v is None else str(v) for v in row))
    print(f"\nTổng số dòng: {len(rows)}")


def main():
    db_path = find_db_path()
    print(f"DB đang kiểm tra: {db_path}")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Kiểm tra bảng bắt buộc
    required_tables = {"units", "users", "user_unit_memberships", "roles", "user_roles"}
    existing_tables = {
        r[0] for r in q_all(
            conn,
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
    }
    missing = required_tables - existing_tables
    if missing:
        raise RuntimeError(f"DB thiếu bảng bắt buộc: {sorted(missing)}")

    print_section("1. TOÀN BỘ UNIT CẤP 2")
    units_lv2 = q_all(
        conn,
        """
        SELECT
            u.id,
            u.ten_don_vi,
            u.cap_do,
            u.parent_id,
            pu.ten_don_vi AS parent_name,
            u.trang_thai,
            u.order_index
        FROM units u
        LEFT JOIN units pu ON pu.id = u.parent_id
        WHERE u.cap_do = 2
        ORDER BY u.order_index, u.ten_don_vi
        """
    )
    print_rows(
        units_lv2,
        headers=["unit_id", "ten_don_vi", "cap_do", "parent_id", "parent_name", "trang_thai", "order_index"]
    )

    print_section("2. TOÀN BỘ UNIT CẤP 3")
    units_lv3 = q_all(
        conn,
        """
        SELECT
            u.id,
            u.ten_don_vi,
            u.cap_do,
            u.parent_id,
            pu.ten_don_vi AS parent_name,
            u.trang_thai,
            u.order_index
        FROM units u
        LEFT JOIN units pu ON pu.id = u.parent_id
        WHERE u.cap_do = 3
        ORDER BY pu.ten_don_vi, u.order_index, u.ten_don_vi
        """
    )
    print_rows(
        units_lv3,
        headers=["unit_id", "ten_don_vi", "cap_do", "parent_id", "parent_name", "trang_thai", "order_index"]
    )

    print_section("3. TOÀN BỘ USER")
    users = q_all(
        conn,
        """
        SELECT
            u.id,
            u.username,
            COALESCE(u.full_name, '') AS full_name,
            COALESCE(u.email, '') AS email,
            COALESCE(u.phone, '') AS phone,
            u.status,
            COALESCE(u.created_at, '') AS created_at
        FROM users u
        ORDER BY u.username
        """
    )
    print_rows(
        users,
        headers=["user_id", "username", "full_name", "email", "phone", "status", "created_at"]
    )

    print_section("4. USER ↔ UNIT MEMBERSHIP")
    memberships = q_all(
        conn,
        """
        SELECT
            us.username,
            COALESCE(us.full_name, '') AS full_name,
            um.user_id,
            um.unit_id,
            un.ten_don_vi,
            un.cap_do,
            COALESCE(pu.ten_don_vi, '') AS parent_unit,
            COALESCE(um.is_primary, 0) AS is_primary,
            COALESCE(um.is_active, 0) AS is_active,
            COALESCE(um.membership_type, '') AS membership_type,
            COALESCE(um.job_title, '') AS job_title
        FROM user_unit_memberships um
        JOIN users us ON us.id = um.user_id
        JOIN units un ON un.id = um.unit_id
        LEFT JOIN units pu ON pu.id = un.parent_id
        ORDER BY un.cap_do, un.ten_don_vi, us.username
        """
    )
    print_rows(
        memberships,
        headers=[
            "username", "full_name", "user_id", "unit_id", "ten_don_vi", "cap_do",
            "parent_unit", "is_primary", "is_active", "membership_type", "job_title"
        ]
    )

    print_section("5. USER ↔ ROLE")
    user_roles = q_all(
        conn,
        """
        SELECT
            us.username,
            COALESCE(us.full_name, '') AS full_name,
            r.code,
            r.name
        FROM user_roles ur
        JOIN users us ON us.id = ur.user_id
        JOIN roles r ON r.id = ur.role_id
        ORDER BY us.username, r.code
        """
    )
    print_rows(
        user_roles,
        headers=["username", "full_name", "role_code", "role_name"]
    )

    print_section("6. USER THEO TỪNG UNIT CẤP 2")
    users_by_lv2 = q_all(
        conn,
        """
        SELECT
            un.ten_don_vi AS unit_name,
            us.username,
            COALESCE(us.full_name, '') AS full_name,
            GROUP_CONCAT(DISTINCT r.code) AS roles,
            COALESCE(um.is_primary, 0) AS is_primary
        FROM user_unit_memberships um
        JOIN units un ON un.id = um.unit_id
        JOIN users us ON us.id = um.user_id
        LEFT JOIN user_roles ur ON ur.user_id = us.id
        LEFT JOIN roles r ON r.id = ur.role_id
        WHERE un.cap_do = 2
        GROUP BY un.ten_don_vi, us.username, us.full_name, um.is_primary
        ORDER BY un.ten_don_vi, us.username
        """
    )
    print_rows(
        users_by_lv2,
        headers=["unit_name", "username", "full_name", "roles", "is_primary"]
    )

    print_section("7. USER THEO TỪNG UNIT CẤP 3")
    users_by_lv3 = q_all(
        conn,
        """
        SELECT
            pu.ten_don_vi AS parent_unit,
            un.ten_don_vi AS unit_name,
            us.username,
            COALESCE(us.full_name, '') AS full_name,
            GROUP_CONCAT(DISTINCT r.code) AS roles,
            COALESCE(um.is_primary, 0) AS is_primary
        FROM user_unit_memberships um
        JOIN units un ON un.id = um.unit_id
        LEFT JOIN units pu ON pu.id = un.parent_id
        JOIN users us ON us.id = um.user_id
        LEFT JOIN user_roles ur ON ur.user_id = us.id
        LEFT JOIN roles r ON r.id = ur.role_id
        WHERE un.cap_do = 3
        GROUP BY pu.ten_don_vi, un.ten_don_vi, us.username, us.full_name, um.is_primary
        ORDER BY pu.ten_don_vi, un.ten_don_vi, us.username
        """
    )
    print_rows(
        users_by_lv3,
        headers=["parent_unit", "unit_name", "username", "full_name", "roles", "is_primary"]
    )

    print_section("8. TÓM TẮT SỐ LƯỢNG")
    summary = q_all(
        conn,
        """
        SELECT 'units_cap_2' AS item, COUNT(*) AS total FROM units WHERE cap_do = 2
        UNION ALL
        SELECT 'units_cap_3' AS item, COUNT(*) AS total FROM units WHERE cap_do = 3
        UNION ALL
        SELECT 'users' AS item, COUNT(*) AS total FROM users
        UNION ALL
        SELECT 'memberships' AS item, COUNT(*) AS total FROM user_unit_memberships
        UNION ALL
        SELECT 'user_roles' AS item, COUNT(*) AS total FROM user_roles
        """
    )
    print_rows(summary, headers=["item", "total"])

    conn.close()
    print("\nHoàn tất kiểm tra.")


if __name__ == "__main__":
    main()