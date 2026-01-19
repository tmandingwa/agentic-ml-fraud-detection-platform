from datetime import datetime, timezone
from uuid import uuid4
import time

from app.agents_detection import score_risk
from app.agents_investigation import build_evidence, investigator_rationale, pack_evidence_json
from app.agents_decision import decide
from app.agents_reporting import make_report_md, write_pdf
from app.repo import insert_txn, fetch_recent_account_txns, fetch_reuse_txns, insert_case
from app.config import CASE_PDF_DIR

def make_case_id() -> str:
    return f"C{uuid4().hex[:12]}"

def txn_to_json(txn: dict) -> dict:
    out = dict(txn)
    if hasattr(out.get("ts"), "isoformat"):
        out["ts"] = out["ts"].isoformat()
    return out

async def process_txn(txn: dict) -> dict:
    t0 = time.perf_counter()

    await insert_txn(txn)

    risk = score_risk(txn)
    txn_event = {"type": "txn", "txn": txn_to_json(txn), "risk": risk}

    # add latency (ms) so UI can display system efficiency
    latency_ms = int((time.perf_counter() - t0) * 1000)
    txn_event["latency_ms"] = latency_ms

    if risk["risk_level"] in ("HIGH", "CRITICAL"):
        recent = await fetch_recent_account_txns(txn["account_id"], limit=120)
        reuse = await fetch_reuse_txns(txn["ip_address"], txn["device_id"], limit=200)

        txn_json = txn_to_json(txn)
        evidence = build_evidence(txn=txn_json, risk=risk, recent_account_txns=recent, reuse_txns=reuse)
        rationale = investigator_rationale(txn=txn_json, risk=risk, evidence=evidence)

        dec = decide(txn=txn_json, risk=risk, evidence=evidence)

        case = {
            "case_id": make_case_id(),
            "created_at": datetime.now(timezone.utc),
            "txn": txn_json,
            "decision": dec["decision"],
            "recommended_action": dec["recommended_action"],
            "evidence": {**evidence, "risk_score": risk["risk_score"], "risk_level": risk["risk_level"]},
            "rationale": rationale,
        }

        report_md = make_report_md({
            "case_id": case["case_id"],
            "created_at": case["created_at"].isoformat(),
            "txn": case["txn"],
            "decision": case["decision"],
            "recommended_action": case["recommended_action"],
            "evidence": case["evidence"],
            "rationale": case["rationale"],
        })

        pdf_path = f"{CASE_PDF_DIR}/{case['case_id']}.pdf"
        write_pdf(report_md, pdf_path)

        evidence_json = pack_evidence_json(case["evidence"])
        await insert_case(case, pdf_path=pdf_path, report_md=report_md, evidence_json=evidence_json)

        alert_event = {
            "type": "alert",
            "case_id": case["case_id"],
            "decision": case["decision"],
            "risk_level": risk["risk_level"],
            "risk_score": risk["risk_score"],
            "report_md": report_md,
            "pdf_path": pdf_path,
            "latency_ms": latency_ms,
        }
        return {"txn_event": txn_event, "alert_event": alert_event}

    return {"txn_event": txn_event, "alert_event": None}
