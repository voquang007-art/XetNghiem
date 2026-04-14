import os
import sys
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime


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
    backup_path = db_path.with_name(f"{db_path.stem}_backup_{ts}{db_path.suffix}")
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


def print_title(title: str):
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)


def parse_indexes(raw: str, max_index: int):
    raw = (raw or "").strip()
    if not raw:
        return []

    selected = set()
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    for part in parts:
        if "-" in part:
            a, b = part.split("-", 1)
            if a.strip().isdigit() and b.strip().isdigit():
                start = int(a.strip())
                end = int(b.strip())
                if start > end:
                    start, end = end, start
                for i in range(start, end + 1):
                    if 1 <= i <= max_index:
                        selected.add(i)
        else:
            if part.isdigit():
                i = int(part)
                if 1 <= i <= max_index:
                    selected.add(i)
    return sorted(selected)


def list_units(conn: sqlite3.Connection, level: int):
    return q_all(
        conn,
        """
        SELECT
            u.id,
            u.ten_don_vi,
            u.cap_do,
            COALESCE(p.ten_don_vi, '') AS parent_name,
            COALESCE(u.trang_thai, '') AS trang_thai,
            COALESCE(u.order_index, 0) AS order_index
        FROM units u
        LEFT JOIN units p ON p.id = u.parent_id
        WHERE u.cap_do = ?
        ORDER BY u.ten_don_vi, u.order_index, u.id
        """,
        (level,),
    )


def list_users(conn: sqlite3.Connection):
    return q_all(
        conn,
        """
        SELECT
            u.id,
            u.username,
            COALESCE(u.full_name, '') AS full_name,
            COALESCE(u.status, '') AS status,
            COALESCE(u.created_at, '') AS created_at
        FROM users u
        ORDER BY u.username
        """
    )


def count_unit_refs(conn: sqlite3.Connection, unit_id: str):
    child_count = q_one(conn, "SELECT COUNT(*) FROM units WHERE parent_id = ?", (unit_id,))[0]
    membership_count = q_one(conn, "SELECT COUNT(*) FROM user_unit_memberships WHERE unit_id = ?", (unit_id,))[0]
    plan_count = 0
    task_count = 0
    file_count = 0

    tables = {r[0] for r in q_all(conn, "SELECT name FROM sqlite_master WHERE type='table'")}
    if "plans" in tables:
        plan_count = q_one(conn, "SELECT COUNT(*) FROM plans WHERE unit_id = ?", (unit_id,))[0]
    if "tasks" in tables:
        try:
            task_count = q_one(conn, "SELECT COUNT(*) FROM tasks WHERE unit_id = ?", (unit_id,))[0]
        except Exception:
            task_count = 0
    if "files" in tables:
        try:
            file_count = q_one(conn, "SELECT COUNT(*) FROM files WHERE unit_id = ?", (unit_id,))[0]
        except Exception:
            file_count = 0

    return {
        "children": child_count,
        "memberships": membership_count,
        "plans": plan_count,
        "tasks": task_count,
        "files": file_count,
    }


def count_user_refs(conn: sqlite3.Connection, user_id: str):
    memberships = q_one(conn, "SELECT COUNT(*) FROM user_unit_memberships WHERE user_id = ?", (user_id,))[0]
    roles = q_one(conn, "SELECT COUNT(*) FROM user_roles WHERE user_id = ?", (user_id,))[0]

    tables = {r[0] for r in q_all(conn, "SELECT name FROM sqlite_master WHERE type='table'")}
    plans = tasks = reports = 0

    if "plans" in tables:
        try:
            plans = q_one(conn, "SELECT COUNT(*) FROM plans WHERE created_by = ?", (user_id,))[0]
        except Exception:
            plans = 0

    if "tasks" in tables:
        for col in ("created_by", "creator_user_id", "owner_user_id", "assignee_id", "assigned_user_id", "assigned_to_user_id", "receiver_user_id"):
            try:
                val = q_one(conn, f"SELECT COUNT(*) FROM tasks WHERE {col} = ?", (user_id,))[0]
                tasks += val
            except Exception:
                continue

    if "task_reports" in tables:
        for col in ("user_id", "created_by"):
            try:
                val = q_one(conn, f"SELECT COUNT(*) FROM task_reports WHERE {col} = ?", (user_id,))[0]
                reports += val
            except Exception:
                continue

    return {
        "memberships": memberships,
        "roles": roles,
        "plans": plans,
        "tasks": tasks,
        "reports": reports,
    }


def delete_unit(conn: sqlite3.Connection, unit_id: str):
    tables = {r[0] for r in q_all(conn, "SELECT name FROM sqlite_master WHERE type='table'")}

    # Chặn nếu còn unit con
    child_count = q_one(conn, "SELECT COUNT(*) FROM units WHERE parent_id = ?", (unit_id,))[0]
    if child_count > 0:
        raise RuntimeError(f"Không thể xóa unit này vì còn {child_count} unit con.")

    # Chặn nếu còn plans/tasks/files tham chiếu
    if "plans" in tables:
        try:
            cnt = q_one(conn, "SELECT COUNT(*) FROM plans WHERE unit_id = ?", (unit_id,))[0]
            if cnt > 0:
                raise RuntimeError(f"Không thể xóa unit này vì còn {cnt} kế hoạch tham chiếu.")
        except sqlite3.OperationalError:
            pass

    if "tasks" in tables:
        try:
            cnt = q_one(conn, "SELECT COUNT(*) FROM tasks WHERE unit_id = ?", (unit_id,))[0]
            if cnt > 0:
                raise RuntimeError(f"Không thể xóa unit này vì còn {cnt} công việc tham chiếu.")
        except sqlite3.OperationalError:
            pass

    if "files" in tables:
        try:
            cnt = q_one(conn, "SELECT COUNT(*) FROM files WHERE unit_id = ?", (unit_id,))[0]
            if cnt > 0:
                raise RuntimeError(f"Không thể xóa unit này vì còn {cnt} tài liệu tham chiếu.")
        except sqlite3.OperationalError:
            pass

    exec_sql(conn, "DELETE FROM user_unit_memberships WHERE unit_id = ?", (unit_id,))
    exec_sql(conn, "DELETE FROM units WHERE id = ?", (unit_id,))


def delete_user(conn: sqlite3.Connection, user_id: str):
    row = q_one(conn, "SELECT username FROM users WHERE id = ?", (user_id,))
    if not row:
        raise RuntimeError("Không tìm thấy user.")
    username = str(row[0] or "").strip().lower()
    if username == "admin":
        raise RuntimeError("Không được xóa user admin.")

    tables = {r[0] for r in q_all(conn, "SELECT name FROM sqlite_master WHERE type='table'")}

    # Chặn nếu còn dữ liệu nghiệp vụ do user tạo
    if "plans" in tables:
        try:
            cnt = q_one(conn, "SELECT COUNT(*) FROM plans WHERE created_by = ?", (user_id,))[0]
            if cnt > 0:
                raise RuntimeError(f"Không thể xóa user này vì còn {cnt} kế hoạch do user tạo.")
        except sqlite3.OperationalError:
            pass

    if "tasks" in tables:
        for col in ("created_by", "creator_user_id", "owner_user_id", "assignee_id", "assigned_user_id", "assigned_to_user_id", "receiver_user_id"):
            try:
                cnt = q_one(conn, f"SELECT COUNT(*) FROM tasks WHERE {col} = ?", (user_id,))[0]
                if cnt > 0:
                    raise RuntimeError(f"Không thể xóa user này vì còn {cnt} bản ghi tasks tham chiếu qua cột {col}.")
            except sqlite3.OperationalError:
                continue

    if "task_reports" in tables:
        for col in ("user_id", "created_by"):
            try:
                cnt = q_one(conn, f"SELECT COUNT(*) FROM task_reports WHERE {col} = ?", (user_id,))[0]
                if cnt > 0:
                    raise RuntimeError(f"Không thể xóa user này vì còn {cnt} báo cáo công việc tham chiếu qua cột {col}.")
            except sqlite3.OperationalError:
                continue

    exec_sql(conn, "DELETE FROM user_unit_memberships WHERE user_id = ?", (user_id,))
    exec_sql(conn, "DELETE FROM user_roles WHERE user_id = ?", (user_id,))
    exec_sql(conn, "DELETE FROM users WHERE id = ?", (user_id,))


def menu():
    print("\nChọn chức năng:")
    print("1. Xem Unit cấp 2")
    print("2. Xem Unit cấp 3")
    print("3. Xem User")
    print("4. Xóa Unit cấp 2 theo chọn")
    print("5. Xóa Unit cấp 3 theo chọn")
    print("6. Xóa User theo chọn")
    print("0. Thoát")


def show_units(conn: sqlite3.Connection, level: int):
    rows = list_units(conn, level)
    print_title(f"DANH SÁCH UNIT CẤP {level}")
    for i, row in enumerate(rows, start=1):
        unit_id, ten_don_vi, cap_do, parent_name, trang_thai, order_index = row
        refs = count_unit_refs(conn, unit_id)
        print(
            f"{i:>3}. {ten_don_vi} | cap_do={cap_do} | parent={parent_name} | "
            f"status={trang_thai} | order={order_index} | "
            f"child={refs['children']} | mem={refs['memberships']} | "
            f"plans={refs['plans']} | tasks={refs['tasks']} | files={refs['files']} | id={unit_id}"
        )
    print(f"\nTổng: {len(rows)}")
    return rows


def show_users(conn: sqlite3.Connection):
    rows = list_users(conn)
    print_title("DANH SÁCH USER")
    for i, row in enumerate(rows, start=1):
        user_id, username, full_name, status, created_at = row
        refs = count_user_refs(conn, user_id)
        print(
            f"{i:>3}. {username} | {full_name} | status={status} | "
            f"memberships={refs['memberships']} | roles={refs['roles']} | "
            f"plans={refs['plans']} | tasks={refs['tasks']} | reports={refs['reports']} | "
            f"id={user_id}"
        )
    print(f"\nTổng: {len(rows)}")
    return rows


def delete_selected_units(conn: sqlite3.Connection, level: int):
    rows = show_units(conn, level)
    if not rows:
        return

    raw = input("\nNhập số thứ tự cần xóa (ví dụ 1,3,5 hoặc 2-4): ").strip()
    idxs = parse_indexes(raw, len(rows))
    if not idxs:
        print("Không có lựa chọn hợp lệ.")
        return

    print_title("CÁC UNIT SẼ XÓA")
    selected = []
    for idx in idxs:
        row = rows[idx - 1]
        selected.append(row)
        print(f"{idx}. {row[1]} | id={row[0]}")

    confirm = input("\nGõ YES để xác nhận xóa: ").strip()
    if confirm != "YES":
        print("Đã hủy.")
        return

    deleted = 0
    failed = 0
    for row in selected:
        unit_id = row[0]
        unit_name = row[1]
        try:
            delete_unit(conn, unit_id)
            print(f"Đã xóa unit: {unit_name} | {unit_id}")
            deleted += 1
        except Exception as ex:
            print(f"Không xóa được unit: {unit_name} | {unit_id} | Lý do: {ex}")
            failed += 1

    conn.commit()
    print(f"\nKết quả: xóa thành công {deleted}, lỗi {failed}.")


def delete_selected_users(conn: sqlite3.Connection):
    rows = show_users(conn)
    if not rows:
        return

    raw = input("\nNhập số thứ tự user cần xóa (ví dụ 1,3,5 hoặc 2-4): ").strip()
    idxs = parse_indexes(raw, len(rows))
    if not idxs:
        print("Không có lựa chọn hợp lệ.")
        return

    print_title("CÁC USER SẼ XÓA")
    selected = []
    for idx in idxs:
        row = rows[idx - 1]
        selected.append(row)
        print(f"{idx}. {row[1]} | {row[2]} | id={row[0]}")

    confirm = input("\nGõ YES để xác nhận xóa: ").strip()
    if confirm != "YES":
        print("Đã hủy.")
        return

    deleted = 0
    failed = 0
    for row in selected:
        user_id = row[0]
        username = row[1]
        try:
            delete_user(conn, user_id)
            print(f"Đã xóa user: {username} | {user_id}")
            deleted += 1
        except Exception as ex:
            print(f"Không xóa được user: {username} | {user_id} | Lý do: {ex}")
            failed += 1

    conn.commit()
    print(f"\nKết quả: xóa thành công {deleted}, lỗi {failed}.")


def main():
    db_path = find_db_path()
    print(f"DB đang dùng: {db_path}")

    backup_path = backup_db(db_path)
    print(f"Đã backup DB tại: {backup_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        while True:
            menu()
            choice = input("Nhập lựa chọn: ").strip()

            if choice == "1":
                show_units(conn, 2)
            elif choice == "2":
                show_units(conn, 3)
            elif choice == "3":
                show_users(conn)
            elif choice == "4":
                delete_selected_units(conn, 2)
            elif choice == "5":
                delete_selected_units(conn, 3)
            elif choice == "6":
                delete_selected_users(conn)
            elif choice == "0":
                print("Thoát.")
                break
            else:
                print("Lựa chọn không hợp lệ.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()