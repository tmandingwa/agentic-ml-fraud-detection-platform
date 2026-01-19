from sqlalchemy import String, Float, Integer, Text, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime, timezone
from app.db import Base

def utcnow():
    return datetime.now(timezone.utc)

class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    txn_id: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    account_id: Mapped[str] = mapped_column(String(16), index=True)  # e.g. 7712-3456
    customer_grade: Mapped[str] = mapped_column(String(16), index=True)  # A/B/C/D
    device_id: Mapped[str] = mapped_column(String(64), index=True)
    ip_address: Mapped[str] = mapped_column(String(64), index=True)

    merchant: Mapped[str] = mapped_column(String(128))
    mcc: Mapped[str] = mapped_column(String(16))
    amount: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(8))
    country: Mapped[str] = mapped_column(String(8))

    channel: Mapped[str] = mapped_column(String(32))  # card_present / card_not_present
    transaction_type: Mapped[str] = mapped_column(String(32), index=True)  # P2P, AIRTIME, etc.
    transaction_status: Mapped[str] = mapped_column(String(32), index=True)  # approved/declined/reversed/chargeback

    home_country: Mapped[str] = mapped_column(String(8))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

Index("idx_txn_account_ts", Transaction.account_id, Transaction.ts)


class Case(Base):
    __tablename__ = "cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    case_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)

    txn_id: Mapped[str] = mapped_column(String(32), index=True)
    account_id: Mapped[str] = mapped_column(String(16), index=True)

    risk_score: Mapped[int] = mapped_column(Integer)
    risk_level: Mapped[str] = mapped_column(String(16))

    decision: Mapped[str] = mapped_column(String(16))  # APPROVE/REVIEW/BLOCK
    recommended_action: Mapped[str] = mapped_column(String(256))

    rationale: Mapped[str] = mapped_column(Text)
    evidence_json: Mapped[str] = mapped_column(Text)

    report_md: Mapped[str] = mapped_column(Text)
    report_pdf_path: Mapped[str] = mapped_column(String(512))
