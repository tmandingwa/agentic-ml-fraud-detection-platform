import asyncio
import os
import json
from typing import Set
from collections import deque

from fastapi import FastAPI, WebSocket, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
from starlette.websockets import WebSocketDisconnect

# ✅ NEW: serve /static
from starlette.staticfiles import StaticFiles

from app.config import SIM_ENABLED, SIM_TPS, CASE_PDF_DIR, APP_BASE_URL
from app.repo import (
    init_db,
    list_cases,
    get_case,
    daily_volume,
    hourly_today,
    system_metrics,
    insert_txn,
    purge_old_data,   # ✅ NEW: retention cleanup
)
from app.simulator import stream_transactions, seed_historical_transactions
from app.pipeline import process_txn

app = FastAPI(title="Agentic Fraud Investigator (Live)")
templates = Jinja2Templates(directory="templates")
clients: Set[WebSocket] = set()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ✅ Railway healthcheck endpoint
@app.get("/health")
async def health():
    return {"status": "ok"}


# ✅ NEW: static directory + resume path (doesn't break anything if file missing)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))           # .../app
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))    # repo root
STATIC_DIR = os.path.join(PROJECT_ROOT, "static")
RESUME_FILENAME = "Timothy_Mandingwa_Resume.pdf"
RESUME_PATH = os.path.join(STATIC_DIR, RESUME_FILENAME)

# ✅ NEW: mount /static so /static/Timothy_Mandingwa_Resume.pdf works
# Note: only mount if folder exists (prevents crash on misdeploy)
if os.path.isdir(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
else:
    print(f"[startup] static dir not found: {STATIC_DIR}")

# lightweight in-memory perf signals (for UI)
latency_window = deque(maxlen=200)   # ms
alerts_window = deque(maxlen=200)
tx_window = deque(maxlen=200)

# ✅ NEW: retention settings (keep only last 7 days)
RETENTION_DAYS = 7
RETENTION_INTERVAL_HOURS = 6  # run cleanup every 6 hours


async def retention_loop():
    """Background job to keep DB storage bounded."""
    while True:
        try:
            result = await purge_old_data(days=RETENTION_DAYS)
            print(f"[retention] purged old rows: {result}")
        except Exception as e:
            # don't crash the app if cleanup fails
            print("[retention] cleanup failed:", repr(e))
        await asyncio.sleep(RETENTION_INTERVAL_HOURS * 60 * 60)


@app.on_event("startup")
async def on_startup():
    os.makedirs(CASE_PDF_DIR, exist_ok=True)
    await init_db()

    # ✅ NEW: run cleanup once on boot (important after redeploy)
    try:
        result = await purge_old_data(days=RETENTION_DAYS)
        print(f"[startup retention] purged old rows: {result}")
    except Exception as e:
        print("[startup retention] cleanup failed:", repr(e))

    # ✅ NEW: start periodic cleanup in background
    asyncio.create_task(retention_loop())

    # Seed 14 days historical (for graphs)
    # NOTE: safe because txn_id is uuid-based now (no duplicates)
    await seed_historical_transactions(insert_txn_fn=insert_txn, days=7, target_total=12000)

    if SIM_ENABLED:
        asyncio.create_task(sim_loop())


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# ✅ NEW: a dedicated resume download endpoint (forces download)
@app.get("/resume")
async def download_resume():
    if not os.path.exists(RESUME_PATH):
        return JSONResponse(
            {
                "error": "resume_missing",
                "expected_path": RESUME_PATH,
                "hint": "Ensure the file exists in the repo at /static/Timothy_Mandingwa_Resume.pdf",
            },
            status_code=404,
        )

    headers = {
        "Content-Disposition": f'attachment; filename="{RESUME_FILENAME}"'
    }
    return FileResponse(
        RESUME_PATH,
        media_type="application/pdf",
        filename=RESUME_FILENAME,
        headers=headers,
    )


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.discard(websocket)
    except Exception:
        clients.discard(websocket)


async def broadcast(payload: dict):
    if not clients:
        return
    msg = json.dumps(payload, default=str)
    dead = []
    for ws in clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def sim_loop():
    async for txn in stream_transactions(tps=SIM_TPS):
        try:
            result = await process_txn(txn)

            # perf tracking
            tx_window.append(1)
            if "latency_ms" in result["txn_event"]:
                latency_window.append(int(result["txn_event"]["latency_ms"]))

            await broadcast(result["txn_event"])

            if result["alert_event"]:
                alerts_window.append(1)

                # ✅ FIX: use relative URL so browser doesn't try to resolve "api" as a domain
                result["alert_event"]["pdf_url"] = f"/api/cases/{result['alert_event']['case_id']}/pdf?download=1"
                await broadcast(result["alert_event"])

        except Exception as e:
            print("SIM LOOP ERROR:", repr(e))
            await asyncio.sleep(0.25)


@app.get("/api/cases")
async def api_cases(limit: int = 50):
    rows = await list_cases(limit=limit)
    for r in rows:
        # ✅ FIX: relative URL
        r["pdf_url"] = f"/api/cases/{r['case_id']}/pdf?download=1"
    return JSONResponse(rows)


@app.get("/api/cases/{case_id}")
async def api_case(case_id: str):
    r = await get_case(case_id)
    if not r:
        return JSONResponse({"error": "not_found"}, status_code=404)

    # ✅ FIX: relative URL
    r["pdf_url"] = f"/api/cases/{case_id}/pdf?download=1"
    return JSONResponse(r)


@app.get("/api/cases/{case_id}/pdf")
async def api_case_pdf(case_id: str, download: bool = Query(True)):
    r = await get_case(case_id)
    if not r:
        return JSONResponse({"error": "not_found"}, status_code=404)
    path = r["pdf_path"]
    if not os.path.exists(path):
        return JSONResponse({"error": "pdf_missing"}, status_code=404)

    # ✅ Force download when download=1/true
    headers = {}
    if download:
        headers["Content-Disposition"] = f'attachment; filename="{case_id}.pdf"'

    return FileResponse(path, media_type="application/pdf", filename=f"{case_id}.pdf", headers=headers)


# -------- Dashboard stats endpoints --------

@app.get("/api/stats/daily_volume")
async def api_daily_volume(days: int = 7, tz: str = Query("UTC")):  # ✅ CHANGED default 14 -> 7
    return JSONResponse(await daily_volume(days=days, tz=tz))


@app.get("/api/stats/hourly_today")
async def api_hourly_today(tz: str = Query("UTC")):
    return JSONResponse(await hourly_today(tz=tz))


@app.get("/api/stats/system")
async def api_system(tz: str = Query("UTC")):
    dbm = await system_metrics(tz=tz)

    avg_latency = (sum(latency_window) / len(latency_window)) if latency_window else None
    alerts_rate = (len(alerts_window) / len(tx_window)) if tx_window else None

    out = {
        **dbm,
        "ws_clients": len(clients),
        "avg_latency_ms_200": (round(avg_latency, 1) if avg_latency is not None else None),
        "alert_rate_recent": (round(alerts_rate, 3) if alerts_rate is not None else None),
    }
    return JSONResponse(out)
