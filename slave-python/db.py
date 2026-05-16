import mysql.connector
from mysql.connector import Error
import datetime
import decimal


def get_connection(dsn: str):
    """
    dsn format: user:password@host:port
    e.g. root:password@127.0.0.1:3306
    """
    user, rest = dsn.split(":", 1)
    password, rest = rest.split("@", 1)
    host, port = rest.rsplit(":", 1)

    return mysql.connector.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        autocommit=True
    )


def _serialize(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def create_database(conn, db_name: str):
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
    cursor.close()


def drop_database(conn, db_name: str):
    cursor = conn.cursor()
    cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    cursor.close()


def create_table(conn, db_name: str, table: str, columns: dict):
    parts = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]
    for col, col_type in columns.items():
        parts.append(f"`{col}` {col_type}")

    query = f"CREATE TABLE IF NOT EXISTS `{db_name}`.`{table}` ({', '.join(parts)})"
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()


def drop_table(conn, db_name: str, table: str):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS `{db_name}`.`{table}`")
    cursor.close()


def insert_row(conn, db_name: str, table: str, data: dict):
    cols = ", ".join(f"`{c}`" for c in data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values = list(data.values())

    query = f"INSERT INTO `{db_name}`.`{table}` ({cols}) VALUES ({placeholders})"
    cursor = conn.cursor()
    cursor.execute(query, values)
    cursor.close()


def upsert_row(conn, db_name: str, table: str, data: dict):
    """
    INSERT ... ON DUPLICATE KEY UPDATE so that replaying a row that already
    exists (e.g. during recovery) is idempotent and never raises a PK error.
    The 'id' column is included in the INSERT so that replica rows keep their
    original ID; it is excluded from the ON DUPLICATE KEY UPDATE clause so we
    never overwrite the PK itself.
    """
    cols         = ", ".join(f"`{c}`" for c in data.keys())
    placeholders = ", ".join(["%s"] * len(data))
    values       = list(data.values())

    updates = ", ".join(
        f"`{c}` = VALUES(`{c}`)"
        for c in data.keys()
        if c != "id"
    )

    query = (
        f"INSERT INTO `{db_name}`.`{table}` ({cols}) VALUES ({placeholders}) "
        f"ON DUPLICATE KEY UPDATE {updates}"
    )
    cursor = conn.cursor()
    cursor.execute(query, values)
    cursor.close()


def select_rows(conn, db_name: str, table: str, condition: str) -> list[dict]:
    query = f"SELECT * FROM `{db_name}`.`{table}`"
    if condition:
        query += f" WHERE {condition}"

    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()

    return [{k: _serialize(v) for k, v in row.items()} for row in rows]


def update_rows(conn, db_name: str, table: str, data: dict, condition: str):
    set_clause = ", ".join(f"`{c}` = %s" for c in data.keys())
    values = list(data.values())

    query = f"UPDATE `{db_name}`.`{table}` SET {set_clause}"
    if condition:
        query += f" WHERE {condition}"

    cursor = conn.cursor()
    cursor.execute(query, values)
    cursor.close()


def delete_rows(conn, db_name: str, table: str, condition: str):
    query = f"DELETE FROM `{db_name}`.`{table}`"
    if condition:
        query += " WHERE " + condition

    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()