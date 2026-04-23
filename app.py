from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel
import redis
import uuid
import time

app = FastAPI(title="Redis Assignments")

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

class LoginRequest(BaseModel):
    user_id: str

class TaskRequest(BaseModel):
    task: str


# ============================================================
# Task 1: Session Storage
# ============================================================

@app.post("/login")
def login(body: LoginRequest):
    session_id = str(uuid.uuid4())
    r.set(f"session:{session_id}", body.user_id, ex=3600)
    return {"session_id": session_id}


@app.get("/me")
def me(x_session_id: str = Header(None)):
    if not x_session_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    user_id = r.get(f"session:{x_session_id}")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    return {"user_id": user_id}


# ============================================================
# Task 2: Rate Limiter (Fixed Window)
# ============================================================

@app.get("/request")
def rate_limited_request(user_id: str):
    key = f"requests:user:{user_id}"
    
    count = r.incr(key)
    if count == 1:
        r.expire(key, 60)
        
    if count > 5:
        raise HTTPException(status_code=429, detail={"error": "rate limit exceeded"})
    
    return {"status": "ok", "remaining": 5 - count}


# ============================================================
# Task 3: Task Queue (FIFO)
# ============================================================

@app.post("/task")
def add_task(body: TaskRequest):
    queue_length = r.lpush("task_queue", body.task)
    return {"status": "queued", "queue_length": queue_length}


@app.get("/task")
def get_task():
    task = r.rpop("task_queue")
    if not task:
        raise HTTPException(status_code=404, detail={"error": "queue is empty"})
    
    return {"task": task}


# ============================================================
# BONUS: Sliding Window Rate Limiter 
# ============================================================

@app.get("/request_sliding")
def rate_limited_request_sliding(user_id: str):
    key = f"requests_sliding:user:{user_id}"
    now = time.time()
    window = 60
    limit = 5

    r.zremrangebyscore(key, 0, now - window)
    
    current_count = r.zcard(key)
    
    if current_count >= limit:
        raise HTTPException(status_code=429, detail={"error": "rate limit exceeded"})

    r.zadd(key, {str(now): now})
    r.expire(key, window)
    
    return {"status": "ok", "remaining": limit - (current_count + 1)}