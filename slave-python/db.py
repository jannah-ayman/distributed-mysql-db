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
    """
    FIX (#7): MySQL Python connector returns types that are not JSON
    serializable: datetime.date, datetime.datetime, decimal.Decimal, bytes.
    Convert them to plain Python types so Flask's jsonify never crashes.
    """
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
    """
    columns: { "name": "VARCHAR(100)", "age": "INT", ... }
    always adds an auto-increment id as primary key
    """
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


def select_rows(conn, db_name: str, table: str, condition: str) -> list[dict]:
    query = f"SELECT * FROM `{db_name}`.`{table}`"
    if condition:
        query += f" WHERE {condition}"

    cursor = conn.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()

    # FIX (#7): apply _serialize to every value so the result is always
    # JSON-safe regardless of column type (DATE, FLOAT, BLOB, etc.).
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
        query += f" WHERE {condition}"

    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()