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

from app.config import SIM_ENABLED, SIM_TPS, CASE_PDF_DIR
from app.repo import init_db, list_cases, get_case, daily_volume, hourly_today, system_metrics, insert_txn
from app.simulator import stream_transactions, seed_historical_transactions
from app.pipeline import process_txn

app = FastAPI(title="Agentic Fraud Investigator (Live)")
templates = Jinja2Templates(directory="templates")
clients: Set[WebSocket] = set()

# lightweight in-memory perf signals (for UI)
latency_window = deque(maxlen=200)   # ms
alerts_window = deque(maxlen=200)
tx_window = deque(maxlen=200)

@app.on_event("startup")
async def on_startup():
    os.makedirs(CASE_PDF_DIR, exist_ok=True)
    await init_db()

    # Seed 14 days historical (for graphs)
    # NOTE: safe because txn_id is uuid-based now (no duplicates)
    await seed_historical_transactions(insert_txn_fn=insert_txn, days=14, target_total=12000)

    if SIM_ENABLED:
        asyncio.create_task(sim_loop())

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

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

                # ✅ Always relative to current host (Railway domain)
                result["alert_event"]["pdf_url"] = f"/api/cases/{result['alert_event']['case_id']}/pdf"
                await broadcast(result["alert_event"])

        except Exception as e:
            print("SIM LOOP ERROR:", repr(e))
            await asyncio.sleep(0.25)

@app.get("/api/cases")
async def api_cases(limit: int = 50):
    rows = await list_cases(limit=limit)
    for r in rows:
        # ✅ Always relative
        r["pdf_url"] = f"/api/cases/{r['case_id']}/pdf"
    return JSONResponse(rows)

@app.get("/api/cases/{case_id}")
async def api_case(case_id: str):
    r = await get_case(case_id)
    if not r:
        return JSONResponse({"error": "not_found"}, status_code=404)
    # ✅ Always relative
    r["pdf_url"] = f"/api/cases/{case_id}/pdf"
    return JSONResponse(r)

@app.get("/api/cases/{case_id}/pdf")
async def api_case_pdf(case_id: str):
    r = await get_case(case_id)
    if not r:
        return JSONResponse({"error": "not_found"}, status_code=404)
    path = r["pdf_path"]
    if not os.path.exists(path):
        return JSONResponse({"error": "pdf_missing"}, status_code=404)

    # filename=... makes Content-Disposition attachment in Starlette
    return FileResponse(path, media_type="application/pdf", filename=f"{case_id}.pdf")

# -------- Dashboard stats endpoints --------

@app.get("/api/stats/daily_volume")
async def api_daily_volume(days: int = 14, tz: str = Query("UTC")):
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
