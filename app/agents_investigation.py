import json
from collections import Counter
from statistics import mean

def build_evidence(txn: dict, risk: dict, recent_account_txns: list, reuse_txns: list) -> dict:
    evidence = {
        "risk_reasons": risk["reasons"],
        "risk_score": risk["risk_score"],
        "risk_level": risk["risk_level"],
        "recent_account_txn_count": len(recent_account_txns),
        "reuse_sample_count": len(reuse_txns),
    }

    recent_amounts = [float(t["amount"]) for t in recent_account_txns[:80]] if recent_account_txns else []
    evidence["acct_avg_amount_80"] = round(mean(recent_amounts), 2) if recent_amounts else 0.0
    evidence["acct_max_amount_80"] = round(max(recent_amounts), 2) if recent_amounts else 0.0

    # Velocity proxy (simple)
    evidence["velocity_proxy_15"] = min(len(recent_account_txns), 15)

    # Status + type distributions
    status_counts = Counter([t.get("transaction_status") for t in recent_account_txns])
    type_counts = Counter([t.get("transaction_type") for t in recent_account_txns])
    evidence["acct_status_counts"] = dict(status_counts)
    evidence["acct_type_counts"] = dict(type_counts)

    # IP/device reuse across accounts
    reuse_accounts = [t["account_id"] for t in reuse_txns]
    evidence["ip_or_device_reuse_accounts_top"] = Counter(reuse_accounts).most_common(6)

    # Baseline deviation
    amt = float(txn["amount"])
    avg = evidence["acct_avg_amount_80"]
    evidence["amount_vs_baseline_ratio"] = round(amt / avg, 2) if avg > 0 else None

    return evidence

def investigator_rationale(txn: dict, risk: dict, evidence: dict) -> list:
    r = []
    r.append(f"Risk {risk['risk_level']} (score={risk['risk_score']}).")

    if txn["country"] != txn.get("home_country"):
        r.append("Geo mismatch relative to account profile.")

    ratio = evidence.get("amount_vs_baseline_ratio")
    if ratio is not None and ratio >= 3.0:
        r.append("Amount significantly above account baseline (>=3x).")

    if evidence.get("velocity_proxy_15", 0) >= 12:
        r.append("High velocity behavior consistent with burst activity.")

    if txn.get("transaction_type") in ("P2P_SEND", "CASHOUT") and txn.get("channel") == "card_not_present":
        r.append("High-risk type combined with CNP channel increases fraud likelihood.")

    top_reuse = evidence.get("ip_or_device_reuse_accounts_top") or []
    if top_reuse and top_reuse[0][1] >= 6:
        r.append("IP/device reuse pattern across multiple accounts is suspicious.")

    if txn.get("transaction_status") == "chargeback":
        r.append("Chargeback observed (strong fraud confirmation signal).")

    return r

def pack_evidence_json(evidence: dict) -> str:
    return json.dumps(evidence, default=str)
