import json
from sqlalchemy import select, desc, or_, func
from sqlalchemy.exc import ProgrammingError
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import asyncio
from sqlalchemy import delete, text

from app.db import SessionLocal, engine, Base
from app.models import Transaction, Case


# ✅ NEW: schema guard so if Railway volume was wiped, tables get recreated automatically
_SCHEMA_READY = False
_SCHEMA_LOCK = asyncio.Lock()

async def _ensure_tables():
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    async with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        await init_db()
        _SCHEMA_READY = True


# ✅ NEW: retry wrapper for "relation does not exist" errors (after wiping DB)
async def _retry_on_missing_table(fn):
    try:
        return await fn()
    except ProgrammingError as e:
        msg = str(e).lower()
        if "does not exist" in msg and ("relation" in msg) and ("transactions" in msg or "cases" in msg):
            # DB wiped -> recreate tables and retry once
            await init_db()
            global _SCHEMA_READY
            _SCHEMA_READY = True
            return await fn()
        raise


async def init_db():
    # IMPORTANT: ensure model classes are imported so Base.metadata is populated
    # (required when DB/volume was wiped and tables need to be recreated)
    from app import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

async def insert_txn(txn: dict):
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def fetch_recent_account_txns(account_id: str, limit: int = 120) -> list:
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def fetch_reuse_txns(ip: str, device: str, limit: int = 200) -> list:
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def insert_case(case: dict, pdf_path: str, report_md: str, evidence_json: str):
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def list_cases(limit: int = 50) -> list:
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def get_case(case_id: str) -> dict | None:
    await _ensure_tables()

    async def _do():
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

    return await _retry_on_missing_table(_do)

# ---------- Stats helpers (timezone aware) ----------

def _safe_tz(tz: str) -> str:
    try:
        ZoneInfo(tz)
        return tz
    except Exception:
        return "UTC"

async def daily_volume(days: int = 14, tz: str = "UTC"):
    await _ensure_tables()
    tz = _safe_tz(tz)
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)

    async def _do():
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

    return await _retry_on_missing_table(_do)

async def hourly_today(tz: str = "UTC"):
    await _ensure_tables()
    tz = _safe_tz(tz)
    now_local = datetime.now(ZoneInfo(tz))
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=ZoneInfo(tz))

    async def _do():
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

    return await _retry_on_missing_table(_do)
#

async def purge_old_data(days: int = 7) -> dict:
    """
    Keep only last `days` of data in transactions + cases.
    Deletes:
      - transactions older than cutoff
      - cases older than cutoff
    """
    await _ensure_tables()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    async def _do():
        async with SessionLocal() as s:
            # delete cases first (if cases reference transactions)
            res_cases = await s.execute(
                delete(Case).where(Case.created_at < cutoff)
            )

            res_txns = await s.execute(
                delete(Transaction).where(Transaction.ts < cutoff)
            )

            await s.commit()

            # Optional: vacuum occasionally (don’t do every request; but ok in periodic job)
            # Note: VACUUM cannot run inside a transaction block in some configs,
            # so execute it carefully. If it fails, ignore.
            try:
                await s.execute(text("VACUUM (ANALYZE)"))
            except Exception:
                pass

        return {
            "cutoff": cutoff.isoformat(),
            "deleted_cases": int(res_cases.rowcount or 0),
            "deleted_txns": int(res_txns.rowcount or 0),
        }

    return await _retry_on_missing_table(_do)


#

async def system_metrics(tz: str = "UTC"):
    await _ensure_tables()
    tz = _safe_tz(tz)
    now_local = datetime.now(ZoneInfo(tz))
    start_today_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=ZoneInfo(tz))
    start_today_utc = start_today_local.astimezone(timezone.utc)

    async def _do():
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

    return await _retry_on_missing_table(_do)
