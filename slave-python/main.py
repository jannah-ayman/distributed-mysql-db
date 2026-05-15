import os
import json
import hashlib
from flask import Flask, request, jsonify
from db import (
    get_connection, create_database, drop_database,
    create_table, drop_table,
    insert_row, select_rows, update_rows, delete_rows
)

AUTH_TOKEN = "my-secret-token-123"
AUTH_TOKEN_HASH = hashlib.sha256(AUTH_TOKEN.encode()).hexdigest()

app = Flask(__name__)

# --- MySQL connection ---
DSN = os.environ.get("MYSQL_DSN", "root:password@127.0.0.1:3306")
conn = get_connection(DSN)
print("✓ Connected to MySQL")
offset = 2
cursor = conn.cursor()
cursor.execute("SET SESSION auto_increment_increment = 2")
cursor.execute(f"SET SESSION auto_increment_offset = {offset}")
cursor.close()
# --- Local metadata copy (master will sync this) ---
local_meta = {"shards": {}}


# ---- Auth helper ----

def authenticate():
    incoming = request.headers.get("X-Auth-Token", "")
    incoming_hash = hashlib.sha256(incoming.encode()).hexdigest()
    return incoming_hash == AUTH_TOKEN_HASH


# ---- Routes ----

@app.route("/ping", methods=["GET"])
def ping():
    if not authenticate():
        return "Unauthorized", 401
    return "pong", 200


@app.route("/internal/exec", methods=["POST"])
def exec_handler():
    if not authenticate():
        return "Unauthorized", 401

    body = request.get_json()
    if not body:
        return jsonify({"success": False, "error": "Invalid request body"}), 400

    db_name   = body.get("db_name", "")
    operation = body.get("operation", "")
    table     = body.get("table", "")
    columns   = body.get("columns", {})
    data      = body.get("data", {})
    condition = body.get("condition", "")
    is_replica = body.get("is_replica", False)

    # if IsReplica, operate on the _replica table
    if is_replica:
        table = table + "_replica"

    try:
        if operation == "CREATE_DB":
            create_database(conn, db_name)
            print(f"  ✓ Created database {db_name}")
            return ok()

        elif operation == "DROP_DB":
            drop_database(conn, db_name)
            print(f"  ✓ Dropped database {db_name}")
            return ok()

        elif operation == "CREATE_TABLE":
            create_table(conn, db_name, table, columns)
            # also create the _replica table if this is a primary table creation
            if not is_replica:
                create_table(conn, db_name, table + "_replica", columns)
            print(f"  ✓ Created table {db_name}.{table}")
            return ok()

        elif operation == "DROP_TABLE":
            drop_table(conn, db_name, table)
            if not is_replica:
                drop_table(conn, db_name, table + "_replica")
            print(f"  ✓ Dropped table {db_name}.{table}")
            return ok()

        elif operation == "INSERT":
            insert_row(conn, db_name, table, data)
            print(f"  ✓ Inserted into {db_name}.{table}")
            return ok()

        elif operation == "SELECT":
            rows = select_rows(conn, db_name, table, condition)
            print(f"  ✓ SELECT from {db_name}.{table} → {len(rows)} rows")
            return ok(rows)

        elif operation == "UPDATE":
            update_rows(conn, db_name, table, data, condition)
            print(f"  ✓ Updated {db_name}.{table} WHERE {condition}")
            return ok()

        elif operation == "DELETE":
            delete_rows(conn, db_name, table, condition)
            print(f"  ✓ Deleted from {db_name}.{table} WHERE {condition}")
            return ok()

        else:
            return err(f"Unknown operation: {operation}")

    except Exception as e:
        return err(str(e))


@app.route("/internal/metadata", methods=["GET"])
def get_metadata():
    if not authenticate():
        return "Unauthorized", 401
    return jsonify(local_meta), 200


@app.route("/internal/sync-metadata", methods=["POST"])
def sync_metadata():
    global local_meta
    if not authenticate():
        return "Unauthorized", 401
    local_meta = request.get_json()
    print("  ✓ Metadata synced from master")
    return "ok", 200


# ---- Response helpers ----

def ok(rows=None):
    resp = {"success": True, "rows": rows or []}
    return jsonify(resp), 200


def err(msg: str):
    return jsonify({"success": False, "error": msg}), 500


# ---- Start ----

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8082))
    print(f"Slave (Python) running on port {port}...")
    app.run(host="0.0.0.0", port=port)