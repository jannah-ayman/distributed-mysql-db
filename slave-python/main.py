import os, json, hashlib, threading, time, requests
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
offset = int(os.environ.get("SLAVE_OFFSET", "2"))
cursor = conn.cursor()
cursor.execute("SET GLOBAL auto_increment_increment = 2")
cursor.execute(f"SET GLOBAL auto_increment_offset = {offset}")
cursor.close()

# --- Local metadata copy (master will sync this) ---
local_meta = {"shards": {}}

master_url = os.environ.get("MASTER_URL", "http://localhost:8095")
acting_as_master = False


# ---- Auth helper ----

def authenticate():
    incoming = request.headers.get("X-Auth-Token", "")
    incoming_hash = hashlib.sha256(incoming.encode()).hexdigest()
    return incoming_hash == AUTH_TOKEN_HASH


def cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Auth-Token"
    return response


@app.after_request
def add_cors(response):
    return cors(response)


# ---- Slave-only routes ----

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

    db_name    = body.get("db_name", "")
    operation  = body.get("operation", "")
    table      = body.get("table", "")
    columns    = body.get("columns", {})
    data       = body.get("data", {})
    condition  = body.get("condition", "")
    is_replica = body.get("is_replica", False)

    if is_replica:
        table = table + "_replica"

    try:
        if operation == "CREATE_DB":
            create_database(conn, db_name)
            return ok()
        elif operation == "DROP_DB":
            drop_database(conn, db_name)
            return ok()
        elif operation == "CREATE_TABLE":
            create_table(conn, db_name, table, columns)
            if not is_replica:
                create_table(conn, db_name, table + "_replica", columns)
            return ok()
        elif operation == "DROP_TABLE":
            drop_table(conn, db_name, table)
            if not is_replica:
                drop_table(conn, db_name, table + "_replica")
            return ok()
        elif operation == "INSERT":
            insert_row(conn, db_name, table, data)
            return ok()
        elif operation == "SELECT":
            rows = select_rows(conn, db_name, table, condition)
            return ok(rows)
        elif operation == "UPDATE":
            update_rows(conn, db_name, table, data, condition)
            return ok()
        elif operation == "DELETE":
            delete_rows(conn, db_name, table, condition)
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


@app.route("/promote", methods=["GET"])
def promote_status():
    return jsonify({"acting_master": acting_as_master}), 200


# ---- Promoted-master routes ----
# These are always registered but return 503 unless acting_as_master is True.
# When the GUI's discoverMaster() hits /ping and gets 200, it switches here.
# Then these routes serve the GUI directly from this slave's own MySQL.

def require_master(f):
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if request.method == "OPTIONS":
            return "", 200
        if not acting_as_master:
            return jsonify({"success": False, "error": "not the master"}), 503
        if not authenticate():
            return "Unauthorized", 401
        return f(*args, **kwargs)
    return wrapper


def merge_rows(*all_rows):
    seen = set()
    merged = []
    for rows in all_rows:
        for row in (rows or []):
            key = str(row.get("id"))
            if key not in seen:
                seen.add(key)
                merged.append(row)
    return merged


@app.route("/db/create", methods=["POST", "OPTIONS"])
@require_master
def promoted_create_db():
    body = request.get_json()
    try:
        create_database(conn, body["db_name"])
        print(f"  [promoted] ✓ Created DB {body['db_name']}")
        return jsonify({"success": True, "message": "Database created"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/db/drop", methods=["DELETE", "OPTIONS"])
@require_master
def promoted_drop_db():
    body = request.get_json()
    try:
        drop_database(conn, body["db_name"])
        print(f"  [promoted] ✓ Dropped DB {body['db_name']}")
        return jsonify({"success": True, "message": "Database dropped"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/create", methods=["POST", "OPTIONS"])
@require_master
def promoted_create_table():
    body = request.get_json()
    try:
        create_table(conn, body["db_name"], body["table"], body.get("columns", {}))
        # keep _replica table in sync
        try:
            create_table(conn, body["db_name"], body["table"] + "_replica", body.get("columns", {}))
        except Exception:
            pass
        local_meta.setdefault("shards", {})[body["table"]] = {
            "shard_1": {"url": "self", "db_name": body["db_name"]}
        }
        return jsonify({"success": True, "message": "Table created"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/drop", methods=["DELETE", "OPTIONS"])
@require_master
def promoted_drop_table():
    body = request.get_json()
    try:
        drop_table(conn, body["db_name"], body["table"])
        try:
            drop_table(conn, body["db_name"], body["table"] + "_replica")
        except Exception:
            pass
        local_meta.get("shards", {}).pop(body["table"], None)
        return jsonify({"success": True, "message": "Table dropped"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/insert", methods=["POST", "OPTIONS"])
@require_master
def promoted_insert():
    body = request.get_json()
    try:
        insert_row(conn, body["db_name"], body["table"], body.get("data", {}))
        return jsonify({"success": True, "message": "Row inserted"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/select", methods=["GET", "OPTIONS"])
@require_master
def promoted_select():
    db_name   = request.args.get("db_name", "")
    table     = request.args.get("table", "")
    condition = request.args.get("condition", "")
    try:
        # Read both primary and replica tables, merge by id.
        rows1 = select_rows(conn, db_name, table, condition)
        try:
            rows2 = select_rows(conn, db_name, table + "_replica", condition)
        except Exception:
            rows2 = []
        merged = merge_rows(rows1, rows2)
        return jsonify({"success": True, "rows": merged}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/update", methods=["PUT", "OPTIONS"])
@require_master
def promoted_update():
    body = request.get_json()
    try:
        update_rows(conn, body["db_name"], body["table"], body.get("data", {}), body.get("condition", ""))
        try:
            update_rows(conn, body["db_name"], body["table"] + "_replica", body.get("data", {}), body.get("condition", ""))
        except Exception:
            pass
        return jsonify({"success": True, "message": "Row updated"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/tables/delete", methods=["DELETE", "OPTIONS"])
@require_master
def promoted_delete():
    body = request.get_json()
    try:
        delete_rows(conn, body["db_name"], body["table"], body.get("condition", ""))
        try:
            delete_rows(conn, body["db_name"], body["table"] + "_replica", body.get("condition", ""))
        except Exception:
            pass
        return jsonify({"success": True, "message": "Row deleted"}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/health", methods=["GET", "OPTIONS"])
@require_master
def promoted_health():
    return jsonify({"master": "promoted-slave", "slaves": []}), 200


# ---- Response helpers ----

def ok(rows=None):
    return jsonify({"success": True, "rows": rows or []}), 200


def err(msg: str):
    return jsonify({"success": False, "error": msg}), 500


# ---- Master watcher ----

def watch_master():
    global acting_as_master
    fails = 0
    while True:
        time.sleep(5)
        try:
            r = requests.get(master_url + "/health",
                             headers={"X-Auth-Token": AUTH_TOKEN}, timeout=3)
            if r.status_code == 200:
                if acting_as_master:
                    print("  ✓ Real master is back — reverting to slave mode")
                    # Push our metadata back to the master.
                    try:
                        requests.post(
                            master_url + "/internal/sync-metadata",
                            json=local_meta,
                            headers={"X-Auth-Token": AUTH_TOKEN},
                            timeout=5
                        )
                        print("  ✓ Pushed metadata to recovered master")
                    except Exception as e:
                        print(f"  ✗ Could not push metadata to master: {e}")
                acting_as_master = False
                fails = 0
                continue
        except Exception:
            pass

        fails += 1
        print(f"  ⚠ Master unreachable ({fails}/3)")
        if fails >= 3 and not acting_as_master:
            acting_as_master = True
            print("  ★ Acting as master now")


threading.Thread(target=watch_master, daemon=True).start()

# ---- Start ----

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8082))
    print(f"Slave (Python) running on port {port}...")
    app.run(host="0.0.0.0", port=port)