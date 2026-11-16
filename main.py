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
        json.dump(JOBS, open(STATE, "w"))
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

class Result(BaseModel):
    status: str
    output: str = ""
    session: str = ""

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
    jid = uuid.uuid4().hex[:12]
    with _lock:
        JOBS[jid] = {"id": jid, "prompt": j.prompt, "thread": j.thread, "session": j.session, "user": j.user,
                     "status": "queued", "created": time.time(), "started": 0, "finished": 0, "output": ""}
        _save()
    return {"id": jid}

@app.get("/jobs/{jid}")
def get_job(jid: str, x_token: str = Header(default="")):
    _auth(x_token)
    j = JOBS.get(jid)
    if not j:
        raise HTTPException(404, "unknown")
    return {k: j[k] for k in ("id", "status", "output", "session", "created", "started", "finished")} | {"worker_seen": LAST["seen"]}

@app.get("/work/next")
def work_next(x_token: str = Header(default="")):
    _auth(x_token)
    LAST["seen"] = time.time()
    with _lock:
        for j in sorted(JOBS.values(), key=lambda x: x["created"]):
            if j["status"] == "queued":
                j["status"] = "running"; j["started"] = time.time(); _save()
                return {"id": j["id"], "prompt": j["prompt"], "thread": j["thread"], "session": j["session"]}
    return {"id": None}

@app.post("/work/{jid}/result")
def work_result(jid: str, r: Result, x_token: str = Header(default="")):
    _auth(x_token)
    with _lock:
        j = JOBS.get(jid)
        if not j:
            raise HTTPException(404, "unknown")
        j["status"] = r.status; j["output"] = r.output; j["session"] = r.session or j["session"]; j["finished"] = time.time()
        # keep the queue small
        old = [k for k, v in JOBS.items() if v["finished"] and time.time() - v["finished"] > 6 * 3600]
        for k in old:
            JOBS.pop(k, None)
        _save()
    return {"ok": True}
