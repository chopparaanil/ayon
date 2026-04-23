from flask import Flask, request, jsonify, make_response
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from werkzeug.security import check_password_hash, generate_password_hash
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
from copy import deepcopy
import json
import os
import re

app = Flask(__name__)

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    return response

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        return make_response(("", 204))

data_backend = os.environ.get("DATA_BACKEND", "memory").lower()
mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017/")
mongo_db = os.environ.get("MONGO_DB", "test")
supabase_url = os.environ.get("SUPABASE_URL", "").rstrip("/")
supabase_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY", "")

client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000) if data_backend == "mongo" else None
db = client[mongo_db] if client is not None else None
collection = db.items if db is not None else None
users_collection = db.users if db is not None else None
shots_collection = db.shots if db is not None else None

memory_items = []
memory_users = {}
memory_shots = {}
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

class SupabaseError(Exception):
    pass

class BackendConfigError(Exception):
    pass

def using_memory():
    return data_backend == "memory"

def using_supabase():
    return data_backend == "supabase"

def using_mongo():
    return data_backend == "mongo"

def ensure_supported_backend():
    if data_backend not in {"memory", "mongo", "supabase"}:
        raise BackendConfigError(
            "DATA_BACKEND must be one of: memory, mongo, supabase"
        )

def supabase_headers(extra=None):
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }

    if extra:
        headers.update(extra)

    return headers

def supabase_request(method, path, payload=None, headers=None):
    if not supabase_url or not supabase_key:
        raise SupabaseError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")

    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(
        f"{supabase_url}/rest/v1/{path}",
        data=body,
        headers=supabase_headers(headers),
        method=method,
    )

    try:
        with urlopen(req, timeout=10) as res:
            text = res.read().decode("utf-8")
            return json.loads(text) if text else None
    except HTTPError as exc:
        details = exc.read().decode("utf-8")
        raise SupabaseError(details or str(exc)) from exc
    except URLError as exc:
        raise SupabaseError(str(exc)) from exc

def supabase_select_one(table, column, value):
    path = f"{table}?select=*&{column}=eq.{quote(value)}&limit=1"
    rows = supabase_request("GET", path)
    return rows[0] if rows else None

def supabase_select_shots():
    return supabase_request("GET", "shots?select=*&order=shot.asc") or []

def supabase_insert_user(user):
    return supabase_request(
        "POST",
        "users",
        user,
        {"Prefer": "return=representation"},
    )

def supabase_upsert_shot(shot_doc):
    return supabase_request(
        "POST",
        "shots?on_conflict=project,sequence,shot,task",
        shot_doc,
        {"Prefer": "resolution=merge-duplicates,return=representation"},
    )

def memory_shot_key(shot_doc):
    return (
        shot_doc["project"],
        shot_doc["sequence"],
        shot_doc["shot"],
        shot_doc["task"],
    )

@app.route('/health', methods=['GET'])
def health():
    try:
        ensure_supported_backend()

        if using_memory():
            return jsonify({"status": "ok", "backend": "memory"})

        if using_supabase():
            supabase_request("GET", "users?select=email&limit=1")
            return jsonify({"status": "ok", "backend": "supabase"})

        client.admin.command('ping')
        return jsonify({"status": "ok", "backend": "mongo"})
    except (PyMongoError, SupabaseError, BackendConfigError) as exc:
        return jsonify({"status": "error", "message": str(exc)}), 503

@app.route('/items', methods=['GET'])
def get_items():
    try:
        ensure_supported_backend()

        if using_memory():
            return jsonify(deepcopy(memory_items))

        items = list(collection.find({}, {"_id": 0}))
        return jsonify(items)
    except (PyMongoError, BackendConfigError) as exc:
        return jsonify({"error": "Database read failed", "details": str(exc)}), 500

@app.route('/items', methods=['POST'])
def add_item():
    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()

    if not name:
        return jsonify({"error": "Field 'name' is required"}), 400

    item = {"name": name}

    try:
        ensure_supported_backend()

        if using_memory():
            memory_items.append(item.copy())
            return jsonify(item), 201

        collection.insert_one(item.copy())
        return jsonify(item), 201
    except (PyMongoError, BackendConfigError) as exc:
        return jsonify({"error": "Database write failed", "details": str(exc)}), 500

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json(silent=True) or {}
    name = str(data.get("name", "")).strip()
    email = str(data.get("email", "")).strip().lower()
    password = str(data.get("password", ""))

    if not name:
        return jsonify({"error": "Name is required"}), 400

    if not EMAIL_RE.match(email):
        return jsonify({"error": "Valid email is required"}), 400

    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    try:
        ensure_supported_backend()

        if using_supabase():
            if supabase_select_one("users", "email", email):
                return jsonify({"error": "Email is already registered"}), 409

            user = {
                "name": name,
                "email": email,
                "password_hash": generate_password_hash(password),
            }
            supabase_insert_user(user)
            return jsonify({"name": name, "email": email}), 201

        if using_memory():
            if email in memory_users:
                return jsonify({"error": "Email is already registered"}), 409

            user = {
                "name": name,
                "email": email,
                "password_hash": generate_password_hash(password),
            }
            memory_users[email] = user
            return jsonify({"name": name, "email": email}), 201

        if users_collection.find_one({"email": email}):
            return jsonify({"error": "Email is already registered"}), 409

        user = {
            "name": name,
            "email": email,
            "password_hash": generate_password_hash(password),
        }
        users_collection.insert_one(user)

        return jsonify({"name": name, "email": email}), 201
    except (PyMongoError, SupabaseError, BackendConfigError) as exc:
        return jsonify({"error": "Registration failed", "details": str(exc)}), 500

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json(silent=True) or {}
    email = str(data.get("email", "")).strip().lower()
    password = str(data.get("password", ""))

    if not EMAIL_RE.match(email):
        return jsonify({"error": "Valid email is required"}), 400

    if not password:
        return jsonify({"error": "Password is required"}), 400

    try:
        ensure_supported_backend()

        if using_supabase():
            user = supabase_select_one("users", "email", email)
        elif using_memory():
            user = memory_users.get(email)
        else:
            user = users_collection.find_one({"email": email})

        if not user or not check_password_hash(user.get("password_hash", ""), password):
            return jsonify({"error": "Invalid email or password"}), 401

        return jsonify({"name": user["name"], "email": user["email"]})
    except (PyMongoError, SupabaseError, BackendConfigError) as exc:
        return jsonify({"error": "Login failed", "details": str(exc)}), 500

@app.route('/shots', methods=['GET'])
def get_shots():
    try:
        ensure_supported_backend()

        if using_supabase():
            return jsonify(supabase_select_shots())

        if using_memory():
            shots = sorted(memory_shots.values(), key=lambda shot: shot["shot"])
            return jsonify(deepcopy(shots))

        shots = list(shots_collection.find({}, {"_id": 0}).sort("shot", 1))
        return jsonify(shots)
    except (PyMongoError, SupabaseError, BackendConfigError) as exc:
        return jsonify({"error": "Unable to load shots", "details": str(exc)}), 500

@app.route('/shots', methods=['POST'])
def add_shot():
    data = request.get_json(silent=True) or {}
    shot_doc = {
        "project": str(data.get("project", "")).strip(),
        "episode": str(data.get("episode", "")).strip(),
        "sequence": str(data.get("sequence", "")).strip(),
        "shot": str(data.get("shot", "")).strip(),
        "task": str(data.get("task", "")).strip(),
        "pipeline_step": str(data.get("pipeline_step", "")).strip(),
        "status": str(data.get("status", "Not Started")).strip(),
        "priority": str(data.get("priority", "Medium")).strip(),
        "artist": str(data.get("artist", "")).strip(),
        "supervisor": str(data.get("supervisor", "")).strip(),
        "start_frame": str(data.get("start_frame", "")).strip(),
        "end_frame": str(data.get("end_frame", "")).strip(),
        "duration": str(data.get("duration", "")).strip(),
        "bid_days": str(data.get("bid_days", "")).strip(),
        "due_date": str(data.get("due_date", "")).strip(),
        "version": str(data.get("version", "")).strip(),
        "notes": str(data.get("notes", "")).strip(),
        "client_feedback": str(data.get("client_feedback", "")).strip(),
    }

    if not shot_doc["shot"]:
        return jsonify({"error": "Shot name is required"}), 400

    if not shot_doc["sequence"]:
        return jsonify({"error": "Sequence is required"}), 400

    if not shot_doc["project"]:
        return jsonify({"error": "Project is required"}), 400

    if not shot_doc["task"]:
        return jsonify({"error": "Task is required"}), 400

    if not shot_doc["pipeline_step"]:
        return jsonify({"error": "Pipeline step is required"}), 400

    if not shot_doc["artist"]:
        shot_doc["artist"] = "Unassigned"

    try:
        ensure_supported_backend()

        if using_supabase():
            supabase_upsert_shot(shot_doc)
            return jsonify(shot_doc), 201

        if using_memory():
            memory_shots[memory_shot_key(shot_doc)] = shot_doc.copy()
            return jsonify(shot_doc), 201

        shots_collection.update_one(
            {
                "project": shot_doc["project"],
                "sequence": shot_doc["sequence"],
                "shot": shot_doc["shot"],
                "task": shot_doc["task"],
            },
            {"$set": shot_doc},
            upsert=True,
        )
        return jsonify(shot_doc), 201
    except (PyMongoError, SupabaseError, BackendConfigError) as exc:
        return jsonify({"error": "Unable to save shot", "details": str(exc)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
