GRADE_RISK = {"A": -5, "B": 0, "C": 8, "D": 15}

HIGH_RISK_TYPES = {"P2P_SEND", "CASHOUT"}     # higher fraud exposure
MED_RISK_TYPES  = {"CASHIN", "MERCHPAY"}      # moderate exposure

HIGH_RISK_STATUS = {"chargeback"}
SOFT_RISK_STATUS = {"reversed", "declined"}

def score_risk(txn: dict) -> dict:
    score = 0
    reasons = []

    # Customer grade
    g = txn.get("customer_grade", "B")
    score += GRADE_RISK.get(g, 0)
    if g in ("C", "D"):
        reasons.append(f"Lower customer grade: {g}")

    # Channel
    if txn["channel"] == "card_not_present":
        score += 18
        reasons.append("Card-not-present")

    # Geo mismatch
    if txn["country"] != txn.get("home_country"):
        score += 22
        reasons.append("Geo mismatch vs home")

    # Transaction type
    ttype = txn.get("transaction_type", "MERCHPAY")
    if ttype in HIGH_RISK_TYPES:
        score += 14
        reasons.append(f"High-risk transaction type: {ttype}")
    elif ttype in MED_RISK_TYPES:
        score += 7
        reasons.append(f"Medium-risk transaction type: {ttype}")

    # Status
    st = txn.get("transaction_status", "approved")
    if st in HIGH_RISK_STATUS:
        score += 35
        reasons.append(f"Fraud-confirming status: {st}")
    elif st in SOFT_RISK_STATUS:
        score += 8
        reasons.append(f"Suspicious status: {st}")

    # Amount thresholds tuned for wallet-type payments
    amt = float(txn["amount"])
    if amt >= 800:
        score += 25
        reasons.append("High amount >= 800")
    elif amt >= 300:
        score += 12
        reasons.append("Amount >= 300")

    # "Test" behavior: tiny airtime recharge (common probing action)
    if ttype == "AIRTIME_RECHARGE" and amt <= 2.0 and txn["channel"] == "card_not_present":
        score += 18
        reasons.append("Probe-like small airtime recharge")

    # Level
    if score >= 85:
        level = "CRITICAL"
    elif score >= 60:
        level = "HIGH"
    elif score >= 35:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {"risk_score": int(score), "risk_level": level, "reasons": reasons}
