from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from pathlib import Path

def make_report_md(case: dict) -> str:
    txn = case["txn"]
    ev = case["evidence"]

    lines = []
    lines.append(f"# Fraud Case Report — {case['case_id']}")
    lines.append(f"- Created: {case['created_at']}")
    lines.append("")
    lines.append("## Transaction")
    lines.append(f"- txn_id: {txn['txn_id']}")
    lines.append(f"- account_id: {txn['account_id']} (grade={txn.get('customer_grade')})")
    lines.append(f"- type: {txn.get('transaction_type')}")
    lines.append(f"- status: {txn.get('transaction_status')}")
    lines.append(f"- amount: {txn['amount']} {txn['currency']}")
    lines.append(f"- merchant: {txn['merchant']} (mcc={txn['mcc']})")
    lines.append(f"- channel: {txn['channel']}")
    lines.append(f"- country: {txn['country']} (home={txn.get('home_country')})")
    lines.append(f"- device_id: {txn['device_id']}")
    lines.append(f"- ip_address: {txn['ip_address']}")
    lines.append("")
    lines.append("## Decision")
    lines.append(f"- Decision: **{case['decision']}**")
    lines.append(f"- Recommended action: {case['recommended_action']}")
    lines.append("")
    lines.append("## Evidence")
    lines.append(f"- Risk level: {ev['risk_level']} (score={ev['risk_score']})")
    lines.append(f"- Reasons: {', '.join(ev.get('risk_reasons') or [])}")
    lines.append(f"- Account avg amount (last 80): {ev.get('acct_avg_amount_80')}")
    lines.append(f"- Velocity proxy (15): {ev.get('velocity_proxy_15')}")
    lines.append(f"- Account status counts: {ev.get('acct_status_counts')}")
    lines.append(f"- Account type counts: {ev.get('acct_type_counts')}")
    lines.append(f"- IP/device reuse top: {ev.get('ip_or_device_reuse_accounts_top')}")
    lines.append("")
    lines.append("## Rationale")
    for x in case["rationale"]:
        lines.append(f"- {x}")

    return "\n".join(lines)

def write_pdf(report_md: str, pdf_path: str) -> str:
    Path(pdf_path).parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(pdf_path, pagesize=LETTER)
    width, height = LETTER

    y = height - 50
    for line in report_md.splitlines():
        if y < 60:
            c.showPage()
            y = height - 50
        c.drawString(50, y, line[:120])
        y -= 14

    c.save()
    return pdf_path
