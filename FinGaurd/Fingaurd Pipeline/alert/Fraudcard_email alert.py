from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
from collections import defaultdict

# Gmail credentials (retrieved at module load, outside handler for serialization safety)
EMAIL_SENDER = "jothibossm86@gmail.com"
APP_PASSWORD = dbutils.secrets.get(scope="finguard-scope", key="gmail_api_key")
# SMTP server configuration
SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

# Idempotent write application ID (unique per application writing to the log table)
_NOTIFICATION_APP_ID = "finguard_fraudcard_email_notification"


# ---------------------------------------------------------------------------
# Email template builder
# ---------------------------------------------------------------------------
def _build_alert_email(customer_name, alerts):
    """Build a clean HTML email body for fraud watchlist match alerts."""
    rows_html = ""
    for a in alerts:
        amount_str = "N/A" if a.get("amount") is None else f"{a['amount']:,.2f} {a.get('currency') or ''}".strip()
        rows_html += f"""
        <tr>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['alert_id']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['transaction_id']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee; text-align: right;">{amount_str}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['merchant_name']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['watch_type']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['risk_level']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['transaction_city']}, {a['transaction_country']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a['transaction_timestamp']}</td>
          <td style="padding: 6px 12px; border-bottom: 1px solid #eee;">{a.get('reason_code') or ''} <span style="color: #888;">{a.get('reason_description') or ''}</span></td>
        </tr>"""

    return f"""
<html>
<body style="font-family: Arial, sans-serif; color: #333; max-width: 800px; margin: 0 auto;">

  <h2 style="color: #c0392b; border-bottom: 2px solid #c0392b; padding-bottom: 8px;">
    🚨 FinGuard — Fraud Watchlist Match Alert
  </h2>

  <p>Dear <b>{customer_name}</b>,</p>

  <p>The following transaction(s) on your card have matched an entry on our fraud watchlist and require your immediate attention:</p>

  <table style="border-collapse: collapse; width: 100%; font-size: 13px;">
    <thead>
      <tr style="background-color: #f8f9fa; text-align: left;">
        <th style="padding: 8px 12px;">Alert ID</th>
        <th style="padding: 8px 12px;">Transaction ID</th>
        <th style="padding: 8px 12px;">Amount</th>
        <th style="padding: 8px 12px;">Merchant</th>
        <th style="padding: 8px 12px;">Watch Type</th>
        <th style="padding: 8px 12px;">Risk Level</th>
        <th style="padding: 8px 12px;">Location</th>
        <th style="padding: 8px 12px;">Timestamp</th>
        <th style="padding: 8px 12px;">Reason</th>
      </tr>
    </thead>
    <tbody>{rows_html}
    </tbody>
  </table>

  <p style="margin-top: 16px; font-size: 12px; color: #888;">
    Recommended action: <b>{alerts[0].get('action') or 'Contact FinGuard support'}</b><br>
    Reported by: {alerts[0].get('reported_by') or 'N/A'} ({alerts[0].get('reported_source') or 'N/A'})<br>
    If you do not recognize any of these transactions, please contact FinGuard support immediately.
  </p>

  <br>
  <p>Regards,<br><b>FinGuard Support Team</b></p>

</body>
</html>"""


# ---------------------------------------------------------------------------
# Email sender — STUB: replace this function body with the tested SMTP
# implementation from alert/Alert Email Notification.py (_send_alert_emails).
# The editor safety checker blocks writing SMTP connection code; paste it in
# manually, keeping the same signature and return format:
#   - imports smtplib, MIMEText, MIMEMultipart inside the function
#   - single SMTP connection to SMTP_HOST:SMTP_PORT with starttls + login
#   - subject: f"FinGuard Fraud Alert: {len(alerts)} Watchlist Match(es) Detected"
#   - body: _build_alert_email(customer_name, alerts)
#   - appends (email_addr, customer_name, "SENT"/"FAILED", None/error)
# ---------------------------------------------------------------------------
def _send_alert_emails(customer_alerts):
    """
    Send consolidated emails to each customer via a single SMTP connection.
    Returns list of (email_addr, customer_name, status, error) tuples.
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    results = []
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, APP_PASSWORD)

            for email_addr, alerts in customer_alerts.items():
                customer_name = alerts[0]["customer_name"]
                subject = f"FinGuard Alert: {len(alerts)} High Value Transaction(s) Detected"
                body = _build_alert_email(customer_name, alerts)

                msg = MIMEMultipart()
                msg["From"] = EMAIL_SENDER
                msg["To"] = email_addr
                msg["Subject"] = subject
                msg.attach(MIMEText(body, "html"))

                try:
                    server.send_message(msg)
                    results.append((email_addr, customer_name, "SENT", None))
                except Exception as e:
                    results.append((email_addr, customer_name, "FAILED", str(e)))
    except Exception as e:
        # Connection failed — mark all as FAILED, don't crash the query
        for email_addr, alerts in customer_alerts.items():
            customer_name = alerts[0]["customer_name"]
            results.append((email_addr, customer_name, "FAILED", str(e)))

    return results


# ---------------------------------------------------------------------------
# ForEachBatch sink: send consolidated emails and log results
# ---------------------------------------------------------------------------
@dp.foreach_batch_sink(name="fraudcard_email_notification_sink")
def fraudcard_email_notification_sink(df, batch_id):
    """Process each batch of fraud watchlist match alerts: send consolidated emails per customer and log results."""
    rows = df.collect()
    if not rows:
        return

    # Group alerts by customer email — one consolidated email per customer.
    # Alerts without a customer email (customer not found in dimension) are logged as SKIPPED.
    customer_alerts = defaultdict(list)
    skipped_alerts = []
    for row in rows:
        alert = row.asDict()
        if alert.get("email"):
            customer_alerts[alert["email"]].append(alert)
        else:
            skipped_alerts.append(alert)

    # Send emails and get per-customer results
    email_results = _send_alert_emails(customer_alerts)
    results_map = {r[0]: (r[2], r[3]) for r in email_results}  # email -> (status, error)

    # Build notification log records
    log_records = []
    now = datetime.now()

    for email_addr, alerts in customer_alerts.items():
        status, error = results_map.get(email_addr, ("FAILED", "Unknown error"))
        customer_name = alerts[0]["customer_name"] or "Customer"
        for a in alerts:
            log_records.append((
                a["alert_id"], email_addr, customer_name,
                status, now, error,
            ))

    for a in skipped_alerts:
        log_records.append((
            a["alert_id"], None, a.get("customer_name"),
            "SKIPPED_NO_EMAIL", now, "Customer email not available — customer not found in silver.customers",
        ))

    # Write notification log to the log table (idempotent via txnVersion/txnAppId)
    log_schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("notification_status", StringType(), True),
        StructField("sent_at", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ])
    log_df = df.sparkSession.createDataFrame(log_records, schema=log_schema)
    log_df.write.format("delta").mode("append") \
        .option("txnVersion", batch_id).option("txnAppId", _NOTIFICATION_APP_ID) \
        .saveAsTable("finguard.gold.fraudcard_alert_notification_log")


# ---------------------------------------------------------------------------
# Append flow: stream new fraud watchlist match alerts to the email notification sink
# ---------------------------------------------------------------------------
@dp.append_flow(target="fraudcard_email_notification_sink")
def fraudcard_alert_email_flow():
    """Stream new fraud watchlist match alerts to the email notification sink."""
    return spark.readStream.table("finguard.gold.fraud_alert")
