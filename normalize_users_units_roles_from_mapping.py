import sys
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime

# =========================
# MAPPING CHUẨN ANH ĐÃ CHỐT
# =========================
TARGET_MAPPING = [
    {"full_name": "Nguyễn A",   "role_code": "ROLE_LANH_DAO",             "unit_name": "HĐTV"},
    {"full_name": "Nguyễn B",   "role_code": "ROLE_BGD",                  "unit_name": "HĐTV"},
    {"full_name": "Trần Thị C", "role_code": "ROLE_KY_THUAT_VIEN_TRUONG", "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn D",   "role_code": "ROLE_TRUONG_KHOA",          "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn E",   "role_code": "ROLE_PHO_TRUONG_KHOA",      "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị F", "role_code": "ROLE_QL_CHAT_LUONG",        "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn G",   "role_code": "ROLE_QL_KY_THUAT",          "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn H",   "role_code": "ROLE_QL_AN_TOAN",           "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn K",   "role_code": "ROLE_QL_VAT_TU",            "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị L", "role_code": "ROLE_QL_TRANG_THIET_BI",    "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn M",   "role_code": "ROLE_QL_MOI_TRUONG",        "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Nguyễn N",   "role_code": "ROLE_QL_CNTT",              "unit_name": "Khoa Xét nghiệm"},
    {"full_name": "Trần Thị O", "role_code": "ROLE_TRUONG_NHOM",          "unit_name": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn P",   "role_code": "ROLE_PHO_NHOM",             "unit_name": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Nguyễn Q",   "role_code": "ROLE_NHAN_VIEN",            "unit_name": "Nhóm Sinh hóa - Miễn dịch"},
    {"full_name": "Trần Thị R", "role_code": "ROLE_TRUONG_NHOM",          "unit_name": "Nhóm Elisa"},
    {"full_name": "Nguyễn S",   "role_code": "ROLE_NHAN_VIEN",            "unit_name": "Nhóm Elisa"},
]

# Các role "vị trí" cần thay thế sạch
POSITION_ROLE_CODES = [
    "ROLE_LANH_DAO",
    "ROLE_BGD",
    "ROLE_TRUONG_KHOA",
    "ROLE_PHO_TRUONG_KHOA",
    "ROLE_KY_THUAT_VIEN_TRUONG",
    "ROLE_QL_CHAT_LUONG",
    "ROLE_QL_KY_THUAT",
    "ROLE_QL_AN_TOAN",
    "ROLE_QL_VAT_TU",
    "ROLE_QL_TRANG_THIET_BI",
    "ROLE_QL_MOI_TRUONG",
    "ROLE_QL_CNTT",
    "ROLE_QL_CONG_VIEC",
    "ROLE_TRUONG_NHOM",
    "ROLE_PHO_NHOM",
    "ROLE_TO_TRUONG",
    "ROLE_PHO_TO",
    "ROLE_NHAN_VIEN",
]

ROLE_NAME_MAP = {
    "ROLE_LANH_DAO": "HĐTV",
    "ROLE_BGD": "BGĐ",
    "ROLE_TRUONG_KHOA": "Trưởng khoa",
    "ROLE_PHO_TRUONG_KHOA": "Phó khoa",
    "ROLE_KY_THUAT_VIEN_TRUONG": "Kỹ thuật viên trưởng",
    "ROLE_QL_CHAT_LUONG": "Quản lý chất lượng",
    "ROLE_QL_KY_THUAT": "Quản lý kỹ thuật",
    "ROLE_QL_AN_TOAN": "Quản lý an toàn",
    "ROLE_QL_VAT_TU": "Quản lý vật tư",
    "ROLE_QL_TRANG_THIET_BI": "Quản lý trang thiết bị",
    "ROLE_QL_MOI_TRUONG": "Quản lý môi trường",
    "ROLE_QL_CNTT": "Quản lý CNTT",
    "ROLE_QL_CONG_VIEC": "Quản lý công việc",
    "ROLE_TRUONG_NHOM": "Nhóm trưởng",
    "ROLE_PHO_NHOM": "Nhóm phó",
    "ROLE_TO_TRUONG": "Tổ trưởng",
    "ROLE_PHO_TO": "Tổ phó",
    "ROLE_NHAN_VIEN": "Nhân viên",
}


def find_db_path() -> Path:
    if len(sys.argv) > 1:
        p = Path(sys.argv[1]).resolve()
        if not p.exists():
            raise FileNotFoundError(f"Không tìm thấy DB: {p}")
        return p

    instance_dir = Path.cwd() / "instance"
    if not instance_dir.exists():
        raise FileNotFoundError(f"Không tìm thấy thư mục instance: {instance_dir}")

    candidates = []
    for pattern in ("*.sqlite3", "*.db", "*.sqlite"):
        candidates.extend(instance_dir.glob(pattern))

    if not candidates:
        raise FileNotFoundError(f"Không tìm thấy file DB trong: {instance_dir}")

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


def backup_db(db_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_name(f"{db_path.stem}_backup_normalize_{ts}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def q_all(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def q_one(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row


def exec_sql(conn: sqlite3.Connection, sql: str, params=()):
    cur = conn.cursor()
    cur.execute(sql, params)
    cur.close()


def get_or_create_role(conn: sqlite3.Connection, role_code: str) -> str:
    row = q_one(conn, "SELECT id FROM roles WHERE UPPER(code) = UPPER(?)", (role_code,))
    if row:
        return row[0]

    role_id = f"role_{role_code.lower()}_{int(datetime.now().timestamp() * 1000)}"
    role_name = ROLE_NAME_MAP.get(role_code, role_code)
    exec_sql(
        conn,
        "INSERT INTO roles (id, code, name) VALUES (?, ?, ?)",
        (role_id, role_code, role_name),
    )
    return role_id


def pick_best_user_by_full_name(conn: sqlite3.Connection, full_name: str):
    rows = q_all(
        conn,
        """
        SELECT
            u.id,
            u.username,
            COALESCE(u.full_name, '') AS full_name,
            COALESCE(u.status, '') AS status,
            COALESCE(u.created_at, '') AS created_at
        FROM users u
        WHERE TRIM(COALESCE(u.full_name, '')) = TRIM(?)
        ORDER BY
            CASE WHEN LOWER(COALESCE(u.username,'')) = 'admin' THEN 1 ELSE 0 END,
            CASE WHEN UPPER(COALESCE(u.status,'')) = 'ACTIVE' THEN 0 ELSE 1 END,
            LENGTH(COALESCE(u.username,'')) ASC,
            COALESCE(u.created_at,'') ASC
        """,
        (full_name,),
    )
    if not rows:
        return None
    return rows[0]


def pick_best_unit_by_name(conn: sqlite3.Connection, unit_name: str):
    rows = q_all(
        conn,
        """
        SELECT
            u.id,
            u.ten_don_vi,
            u.cap_do,
            COALESCE(u.trang_thai, '') AS trang_thai,
            COALESCE(u.order_index, 0) AS order_index,
            COALESCE(p.ten_don_vi, '') AS parent_name
        FROM units u
        LEFT JOIN units p ON p.id = u.parent_id
        WHERE TRIM(COALESCE(u.ten_don_vi, '')) = TRIM(?)
        ORDER BY
            CASE WHEN UPPER(COALESCE(u.trang_thai,'')) = 'ACTIVE' THEN 0 ELSE 1 END,
            u.cap_do ASC,
            u.order_index ASC
        """,
        (unit_name,),
    )
    if not rows:
        return None
    return rows[0]


def parent_unit_of(conn: sqlite3.Connection, unit_id: str):
    return q_one(
        conn,
        """
        SELECT
            p.id,
            p.ten_don_vi,
            p.cap_do,
            COALESCE(p.trang_thai, '') AS trang_thai
        FROM units u
        JOIN units p ON p.id = u.parent_id
        WHERE u.id = ?
        """,
        (unit_id,),
    )


def rebuild_membership_for_position(conn: sqlite3.Connection, user_id: str, role_code: str, primary_unit_id: str):
    unit_row = q_one(
        conn,
        "SELECT id, ten_don_vi, cap_do, parent_id FROM units WHERE id = ?",
        (primary_unit_id,),
    )
    if not unit_row:
        raise RuntimeError("Không tìm thấy unit chính để dựng membership.")

    _unit_id, _unit_name, cap_do, parent_id = unit_row

    # Xóa toàn bộ membership cũ của user
    exec_sql(conn, "DELETE FROM user_unit_memberships WHERE user_id = ?", (user_id,))

    memberships_to_add = []

    if role_code in {"ROLE_LANH_DAO", "ROLE_BGD"}:
        memberships_to_add.append((primary_unit_id, 1, 1))

    elif role_code in {
        "ROLE_TRUONG_KHOA",
        "ROLE_PHO_TRUONG_KHOA",
        "ROLE_KY_THUAT_VIEN_TRUONG",
        "ROLE_QL_CHAT_LUONG",
        "ROLE_QL_KY_THUAT",
        "ROLE_QL_AN_TOAN",
        "ROLE_QL_VAT_TU",
        "ROLE_QL_TRANG_THIET_BI",
        "ROLE_QL_MOI_TRUONG",
        "ROLE_QL_CNTT",
        "ROLE_QL_CONG_VIEC",
    }:
        if int(cap_do) != 2:
            raise RuntimeError(f"Role {role_code} phải gắn vào đơn vị cấp 2.")
        memberships_to_add.append((primary_unit_id, 1, 1))

    elif role_code in {"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM", "ROLE_TO_TRUONG", "ROLE_PHO_TO"}:
        if int(cap_do) != 3:
            raise RuntimeError(f"Role {role_code} phải gắn vào đơn vị cấp 3.")
        memberships_to_add.append((primary_unit_id, 1, 1))
        if parent_id:
            memberships_to_add.append((parent_id, 0, 1))

    elif role_code == "ROLE_NHAN_VIEN":
        if int(cap_do) == 3:
            memberships_to_add.append((primary_unit_id, 1, 1))
            if parent_id:
                memberships_to_add.append((parent_id, 0, 1))
        else:
            memberships_to_add.append((primary_unit_id, 1, 1))

    else:
        memberships_to_add.append((primary_unit_id, 1, 1))

    for idx, (unit_id, is_primary, is_active) in enumerate(memberships_to_add, start=1):
        membership_id = f"mem_{user_id}_{idx}_{int(datetime.now().timestamp() * 1000)}"
        exec_sql(
            conn,
            """
            INSERT INTO user_unit_memberships (
                id, user_id, unit_id, is_primary, is_active
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (membership_id, user_id, unit_id, is_primary, is_active),
        )


def replace_position_role(conn: sqlite3.Connection, user_id: str, role_code: str):
    # Xóa các role vị trí cũ
    role_rows = q_all(
        conn,
        f"""
        SELECT id FROM roles
        WHERE UPPER(code) IN ({",".join("?" for _ in POSITION_ROLE_CODES)})
        """,
        tuple(POSITION_ROLE_CODES),
    )
    role_ids = [r[0] for r in role_rows]
    if role_ids:
        exec_sql(
            conn,
            f"DELETE FROM user_roles WHERE user_id = ? AND role_id IN ({','.join('?' for _ in role_ids)})",
            (user_id, *role_ids),
        )

    new_role_id = get_or_create_role(conn, role_code)
    user_role_id = f"urole_{user_id}_{int(datetime.now().timestamp() * 1000)}"
    exec_sql(
        conn,
        "INSERT INTO user_roles (id, user_id, role_id) VALUES (?, ?, ?)",
        (user_role_id, user_id, new_role_id),
    )


def show_preview(conn: sqlite3.Connection):
    print("\n" + "=" * 110)
    print("XEM TRƯỚC MAPPING SẼ ÁP")
    print("=" * 110)

    for idx, item in enumerate(TARGET_MAPPING, start=1):
        full_name = item["full_name"]
        role_code = item["role_code"]
        unit_name = item["unit_name"]

        user_row = pick_best_user_by_full_name(conn, full_name)
        unit_row = pick_best_unit_by_name(conn, unit_name)

        if not user_row:
            print(f"{idx:>2}. USER KHÔNG TÌM THẤY | {full_name} | {role_code} | {unit_name}")
            continue
        if not unit_row:
            print(f"{idx:>2}. UNIT KHÔNG TÌM THẤY | {full_name} | {role_code} | {unit_name}")
            continue

        print(
            f"{idx:>2}. user={user_row[1]} | full_name={full_name} | "
            f"role={role_code} | unit={unit_row[1]} | cap_do={unit_row[2]} | "
            f"status={unit_row[3]} | unit_id={unit_row[0]}"
        )


def print_final_state(conn: sqlite3.Connection):
    print("\n" + "=" * 110)
    print("TRẠNG THÁI SAU KHI CHUẨN HÓA")
    print("=" * 110)

    rows = q_all(
        conn,
        """
        SELECT
            u.username,
            COALESCE(u.full_name, '') AS full_name,
            r.code AS role_code,
            un.ten_don_vi AS unit_name,
            un.cap_do,
            COALESCE(p.ten_don_vi, '') AS parent_unit,
            COALESCE(m.is_primary, 0) AS is_primary,
            COALESCE(m.is_active, 0) AS is_active
        FROM users u
        LEFT JOIN user_roles ur ON ur.user_id = u.id
        LEFT JOIN roles r ON r.id = ur.role_id
        LEFT JOIN user_unit_memberships m ON m.user_id = u.id
        LEFT JOIN units un ON un.id = m.unit_id
        LEFT JOIN units p ON p.id = un.parent_id
        WHERE TRIM(COALESCE(u.full_name, '')) IN ({})
        ORDER BY u.full_name, is_primary DESC, unit_name
        """.format(",".join("?" for _ in TARGET_MAPPING)),
        tuple(item["full_name"] for item in TARGET_MAPPING),
    )

    for row in rows:
        print(
            f"{row[0]} | {row[1]} | {row[2]} | {row[3]} | cap_do={row[4]} | "
            f"parent={row[5]} | is_primary={row[6]} | is_active={row[7]}"
        )


def main():
    db_path = find_db_path()
    print(f"DB đang dùng: {db_path}")

    backup_path = backup_db(db_path)
    print(f"Đã backup DB tại: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA foreign_keys = OFF")

        show_preview(conn)

        confirm = input("\nGõ APPLY YES để áp mapping chuẩn này: ").strip()
        if confirm != "APPLY YES":
            print("Đã hủy.")
            return

        for item in TARGET_MAPPING:
            full_name = item["full_name"]
            role_code = item["role_code"]
            unit_name = item["unit_name"]

            user_row = pick_best_user_by_full_name(conn, full_name)
            if not user_row:
                print(f"BỎ QUA: không tìm thấy user {full_name}")
                continue

            unit_row = pick_best_unit_by_name(conn, unit_name)
            if not unit_row:
                print(f"BỎ QUA: không tìm thấy unit {unit_name}")
                continue

            user_id = user_row[0]
            username = user_row[1]
            unit_id = unit_row[0]

            replace_position_role(conn, user_id, role_code)
            rebuild_membership_for_position(conn, user_id, role_code, unit_id)

            print(f"Đã chuẩn hóa: {username} | {full_name} | {role_code} | {unit_name}")

        conn.commit()
        print_final_state(conn)
        print("\nHoàn tất.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()