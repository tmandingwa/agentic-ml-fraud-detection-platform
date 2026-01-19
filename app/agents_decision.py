def decide(txn: dict, risk: dict, evidence: dict) -> dict:
    decision = "APPROVE"
    action = "No action"

    if risk["risk_level"] == "CRITICAL":
        decision = "BLOCK"
        action = "Block transaction + lock account + step-up verification"
    elif risk["risk_level"] == "HIGH":
        decision = "REVIEW"
        action = "Queue for manual review + step-up verification"

    # Strong policy: chargeback means confirmed loss signal
    if txn.get("transaction_status") == "chargeback":
        decision = "BLOCK"
        action = "Confirmed fraud signal (chargeback): block + lock + investigation"

    # Escalation: CNP + geo mismatch + high velocity
    if (
        txn.get("channel") == "card_not_present"
        and txn.get("country") != txn.get("home_country")
        and evidence.get("velocity_proxy_15", 0) >= 12
    ):
        decision = "BLOCK"
        action = "High-confidence fraud: CNP + geo mismatch + velocity"

    # Escalation: extreme baseline deviation
    ratio = evidence.get("amount_vs_baseline_ratio")
    if ratio is not None and ratio >= 6.0:
        decision = "BLOCK"
        action = "High-confidence fraud: amount extremely above baseline"

    return {"decision": decision, "recommended_action": action}
