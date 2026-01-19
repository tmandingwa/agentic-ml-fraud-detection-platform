import json
from sqlalchemy import select, desc, or_, func
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from app.db import SessionLocal, engine, Base
from app.models import Transaction, Case

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def insert_txn(txn: dict):
    async with SessionLocal() as s:
        obj = Transaction(
            txn_id=txn["txn_id"],
            ts=txn["ts"],
            account_id=txn["account_id"],
            customer_grade=txn["customer_grade"],
            device_id=txn["device_id"],
            ip_address=txn["ip_address"],
            merchant=txn["merchant"],
            mcc=txn["mcc"],
            amount=float(txn["amount"]),
            currency=txn["currency"],
            country=txn["country"],
            channel=txn["channel"],
            transaction_type=txn["transaction_type"],
            transaction_status=txn["transaction_status"],
            home_country=txn["home_country"],
        )
        s.add(obj)
        await s.commit()

async def fetch_recent_account_txns(account_id: str, limit: int = 120) -> list:
    async with SessionLocal() as s:
        q = (
            select(Transaction)
            .where(Transaction.account_id == account_id)
            .order_by(desc(Transaction.ts))
            .limit(limit)
        )
        rows = (await s.execute(q)).scalars().all()
    return [
        {
            "txn_id": r.txn_id, "ts": r.ts, "account_id": r.account_id,
            "customer_grade": r.customer_grade,
            "device_id": r.device_id, "ip_address": r.ip_address,
            "merchant": r.merchant, "mcc": r.mcc,
            "amount": r.amount, "currency": r.currency,
            "country": r.country, "channel": r.channel,
            "transaction_type": r.transaction_type,
            "transaction_status": r.transaction_status,
        }
        for r in rows
    ]

async def fetch_reuse_txns(ip: str, device: str, limit: int = 200) -> list:
    async with SessionLocal() as s:
        q = (
            select(Transaction)
            .where(or_(Transaction.ip_address == ip, Transaction.device_id == device))
            .order_by(desc(Transaction.ts))
            .limit(limit)
        )
        rows = (await s.execute(q)).scalars().all()
    return [
        {
            "txn_id": r.txn_id, "ts": r.ts, "account_id": r.account_id,
            "customer_grade": r.customer_grade,
            "device_id": r.device_id, "ip_address": r.ip_address,
            "merchant": r.merchant, "amount": r.amount,
            "country": r.country, "channel": r.channel,
            "transaction_type": r.transaction_type,
            "transaction_status": r.transaction_status,
        }
        for r in rows
    ]

async def insert_case(case: dict, pdf_path: str, report_md: str, evidence_json: str):
    async with SessionLocal() as s:
        obj = Case(
            case_id=case["case_id"],
            created_at=case["created_at"],
            txn_id=case["txn"]["txn_id"],
            account_id=case["txn"]["account_id"],
            risk_score=case["evidence"]["risk_score"],
            risk_level=case["evidence"]["risk_level"],
            decision=case["decision"],
            recommended_action=case["recommended_action"],
            rationale="\n".join(case["rationale"]),
            evidence_json=evidence_json,
            report_md=report_md,
            report_pdf_path=pdf_path,
        )
        s.add(obj)
        await s.commit()

async def list_cases(limit: int = 50) -> list:
    async with SessionLocal() as s:
        q = select(Case).order_by(desc(Case.created_at)).limit(limit)
        rows = (await s.execute(q)).scalars().all()

    out = []
    for r in rows:
        out.append({
            "case_id": r.case_id,
            "created_at": r.created_at,
            "txn_id": r.txn_id,
            "account_id": r.account_id,
            "risk_score": r.risk_score,
            "risk_level": r.risk_level,
            "decision": r.decision,
            "recommended_action": r.recommended_action,
            "report_md": r.report_md,
            "evidence": json.loads(r.evidence_json),
            "rationale": r.rationale.splitlines(),
            "pdf_path": r.report_pdf_path,
        })
    return out

async def get_case(case_id: str) -> dict | None:
    async with SessionLocal() as s:
        q = select(Case).where(Case.case_id == case_id).limit(1)
        r = (await s.execute(q)).scalars().first()
    if not r:
        return None
    return {
        "case_id": r.case_id,
        "created_at": r.created_at,
        "txn_id": r.txn_id,
        "account_id": r.account_id,
        "risk_score": r.risk_score,
        "risk_level": r.risk_level,
        "decision": r.decision,
        "recommended_action": r.recommended_action,
        "report_md": r.report_md,
        "evidence": json.loads(r.evidence_json),
        "rationale": r.rationale.splitlines(),
        "pdf_path": r.report_pdf_path,
    }

# ---------- Stats helpers (timezone aware) ----------

def _safe_tz(tz: str) -> str:
    try:
        ZoneInfo(tz)
        return tz
    except Exception:
        return "UTC"

async def daily_volume(days: int = 14, tz: str = "UTC"):
    tz = _safe_tz(tz)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    async with SessionLocal() as s:
        local_ts = func.timezone(tz, Transaction.ts)
        d = func.date_trunc("day", local_ts).label("day")
        q = (
            select(d, func.count(Transaction.id))
            .where(Transaction.ts >= start)
            .group_by(d)
            .order_by(d)
        )
        rows = (await s.execute(q)).all()

    return [{"day": r[0].date().isoformat(), "count": int(r[1])} for r in rows]

async def hourly_today(tz: str = "UTC"):
    tz = _safe_tz(tz)
    now_local = datetime.now(ZoneInfo(tz))
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=ZoneInfo(tz))

    async with SessionLocal() as s:
        local_ts = func.timezone(tz, Transaction.ts)
        h = func.date_trunc("hour", local_ts).label("hour")
        q = (
            select(h, func.count(Transaction.id))
            .where(local_ts >= start_local.replace(tzinfo=None))  # timezone() returns naive local timestamp
            .group_by(h)
            .order_by(h)
        )
        rows = (await s.execute(q)).all()

    return [{"hour": r[0].strftime("%H:00"), "count": int(r[1])} for r in rows]

async def system_metrics(tz: str = "UTC"):
    tz = _safe_tz(tz)
    now_local = datetime.now(ZoneInfo(tz))
    start_today_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=ZoneInfo(tz))
    start_today_utc = start_today_local.astimezone(timezone.utc)

    async with SessionLocal() as s:
        total_txns = (await s.execute(select(func.count(Transaction.id)))).scalar_one()
        total_cases = (await s.execute(select(func.count(Case.id)))).scalar_one()
        today_txns = (await s.execute(select(func.count(Transaction.id)).where(Transaction.ts >= start_today_utc))).scalar_one()
        today_cases = (await s.execute(select(func.count(Case.id)).where(Case.created_at >= start_today_utc))).scalar_one()

        total_chargebacks = (await s.execute(
            select(func.count(Transaction.id)).where(Transaction.transaction_status == "chargeback")
        )).scalar_one()

        chargebacks_flagged = (await s.execute(
            select(func.count(Case.id))
            .join(Transaction, Transaction.txn_id == Case.txn_id)
            .where(Transaction.transaction_status == "chargeback")
        )).scalar_one()

        avg_risk = (await s.execute(select(func.avg(Case.risk_score)))).scalar_one()

    detect_rate = (chargebacks_flagged / total_chargebacks) if total_chargebacks else None

    return {
        "tz": tz,
        "total_txns": int(total_txns),
        "total_cases": int(total_cases),
        "today_txns": int(today_txns),
        "today_cases": int(today_cases),
        "total_chargebacks": int(total_chargebacks),
        "chargebacks_flagged": int(chargebacks_flagged),
        "chargeback_detect_rate": (round(detect_rate, 3) if detect_rate is not None else None),
        "avg_case_risk_score": (round(float(avg_risk), 1) if avg_risk is not None else None),
    }
