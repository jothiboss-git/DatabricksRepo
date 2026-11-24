# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Create test email body
body = """
<html>
<body>

<h2>Test Email from Databricks</h2>

<p>This is a test email sent from Databricks notebook.</p>

<p>If you receive this, the email configuration is working correctly.</p>

<br>

<p>
Thanks<br>
<b>FinGuard Support Team</b>
</p>

</body>
</html>
"""

print(body)

# COMMAND ----------

# DBTITLE 1,Create test subject
subject = "Test Email from Databricks"

print(subject)

# COMMAND ----------

gmail_api_key = dbutils.secrets.get(
            scope="finguard-scope",
            key="gmail_api_key"
        )

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText

# Gmail account
EMAIL = "jothibossm86@gmail.com"
APP_PASSWORD = gmail_api_key

# Receiver
to_email = "jothibossm86@gmail.com"

# Create HTML message directly (this ensures proper HTML rendering)
msg = MIMEText(body, "html", "utf-8")
msg["From"] = EMAIL
msg["To"] = to_email
msg["Subject"] = subject

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL, APP_PASSWORD)
        server.send_message(msg)

    print("✅ Email sent successfully!")

except Exception as e:
    print("❌ Error sending email:", e)