"""Organica relay: a small job queue between the Organica app and an analyst worker.
The app posts a request, the worker pulls it, runs it and posts the answer. One shared token.
State lives in memory with a JSON mirror on disk; the queue is short lived by design."""
import os, json, time, uuid, threading
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

TOKEN = os.environ.get("RELAY_TOKEN", "")
STATE = os.environ.get("RELAY_STATE", "/tmp/relay_state.json")
app = FastAPI(title="organica-relay", docs_url=None, redoc_url=None, openapi_url=None)
_lock = threading.Lock()
try:
    JOBS = json.load(open(STATE))
except Exception:
    JOBS = {}

def _save():
    try:
        slim = {k: {kk: vv for kk, vv in v.items() if kk not in ("files", "files_in")} for k, v in JOBS.items()}
        json.dump(slim, open(STATE, "w"))
    except Exception:
        pass

def _auth(x_token):
    if not TOKEN or x_token != TOKEN:
        raise HTTPException(401, "no")

class NewJob(BaseModel):
    prompt: str
    thread: str = ""
    session: str = ""
    user: str = ""
    files: list = []      # [{"name": str, "b64": str}], 40 MB in total

class Result(BaseModel):
    status: str
    output: str = ""
    session: str = ""
    files: list = []      # [{"name": str, "b64": str}]

MAX_B64 = 56 * 1024 * 1024

@app.get("/health")
def health():
    with _lock:
        q = sum(1 for j in JOBS.values() if j["status"] == "queued")
        r = sum(1 for j in JOBS.values() if j["status"] == "running")
    return {"ok": True, "queued": q, "running": r, "worker_seen": LAST.get("seen", 0)}

LAST = {"seen": 0}

@app.post("/jobs")
def new_job(j: NewJob, x_token: str = Header(default="")):
    _auth(x_token)
    if sum(len(f.get("b64", "")) for f in j.files) > MAX_B64:
        raise HTTPException(413, "files too large")
    jid = uuid.uuid4().hex[:12]
    with _lock:
        if j.thread and not j.session:
            prev = [v for v in JOBS.values() if v.get("thread") == j.thread and v.get("session") and v["status"] in ("done", "error")]
            if prev:
                j.session = max(prev, key=lambda v: v["finished"])["session"]
        JOBS[jid] = {"id": jid, "prompt": j.prompt, "thread": j.thread, "session": j.session, "user": j.user,
                     "status": "queued", "created": time.time(), "started": 0, "finished": 0, "output": "",
                     "files_in": j.files, "files": []}
        _save()
    return {"id": jid}

@app.get("/jobs/{jid}")
def get_job(jid: str, x_token: str = Header(default="")):
    _auth(x_token)
    j = JOBS.get(jid)
    if not j:
        raise HTTPException(404, "unknown")
    return {k: j[k] for k in ("id", "status", "output", "session", "created", "started", "finished")} | {"worker_seen": LAST["seen"], "files": [f["name"] for f in j.get("files", [])]}

@app.get("/jobs/{jid}/files/{name}")
def get_file(jid: str, name: str, x_token: str = Header(default="")):
    _auth(x_token)
    j = JOBS.get(jid)
    if not j:
        raise HTTPException(404, "unknown")
    for f in j.get("files", []):
        if f["name"] == name:
            return {"name": name, "b64": f["b64"]}
    raise HTTPException(404, "no such file")


@app.get("/jobs/{jid}/wait")
def wait_job(jid: str, seconds: int = 50, x_token: str = Header(default="")):
    """Blocks up to `seconds` (max 55) until the job is done or failed, then answers like GET /jobs/{id}. Fewer loop turns for a flow."""
    _auth(x_token)
    deadline = time.time() + max(1, min(seconds, 55))
    while time.time() < deadline:
        j = JOBS.get(jid)
        if not j:
            raise HTTPException(404, "unknown")
        if j["status"] in ("done", "error"):
            break
        time.sleep(2)
    return get_job(jid, x_token)

@app.get("/jobs/{jid}/files/{name}/raw")
def get_file_raw(jid: str, name: str, x_token: str = Header(default="")):
    """The file as bytes with its content type, so a flow can write it straight into OneDrive."""
    _auth(x_token)
    import base64, mimetypes
    from fastapi.responses import Response
    j = JOBS.get(jid)
    if not j:
        raise HTTPException(404, "unknown")
    for f in j.get("files", []):
        if f["name"] == name:
            mt = mimetypes.guess_type(name)[0] or "application/octet-stream"
            return Response(content=base64.b64decode(f["b64"]), media_type=mt, headers={"Content-Disposition": f'attachment; filename="{name}"'})
    raise HTTPException(404, "no such file")

@app.get("/work/next")
def work_next(x_token: str = Header(default="")):
    _auth(x_token)
    LAST["seen"] = time.time()
    with _lock:
        for j in sorted(JOBS.values(), key=lambda x: x["created"]):
            if j["status"] == "queued":
                j["status"] = "running"; j["started"] = time.time(); _save()
                files = j.pop("files_in", [])
                return {"id": j["id"], "prompt": j["prompt"], "thread": j["thread"], "session": j["session"], "files": files}
    return {"id": None}

@app.post("/work/{jid}/result")
def work_result(jid: str, r: Result, x_token: str = Header(default="")):
    _auth(x_token)
    with _lock:
        j = JOBS.get(jid)
        if not j:
            raise HTTPException(404, "unknown")
        j["status"] = r.status; j["output"] = r.output; j["session"] = r.session or j["session"]; j["finished"] = time.time(); j["files"] = r.files
        # keep the queue small
        old = [k for k, v in JOBS.items() if v["finished"] and time.time() - v["finished"] > 6 * 3600]
        for k in old:
            JOBS.pop(k, None)
        _save()
    return {"ok": True}
