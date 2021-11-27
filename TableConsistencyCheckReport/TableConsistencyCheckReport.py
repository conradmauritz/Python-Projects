"""TableConsistencyCheckReport.py: This script connects to a HANA tenant database and retrieves results related to the
table consistency check which is then sent as a report via email"""

__author__ = "Conrad Fourie"
__version__ = "1.0.0"

# Import the required modules
from hdbcli import dbapi
from datetime import datetime
import os
import sys
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Retrieving and formatting the date
now = datetime.now()
today = now.strftime("%d-%m-%Y")

# Beginning of the User Editable Area
hdbuserstore_key = "<KEY>"
smtp_server = "<SMTP HOST>"
smtp_port = "<SMTP PORT>"
email_from = "<FROM ADDRESS>"
email_to = "<TO ADDRESS>"
# email_cc = "<LIST OF ADDRESSES SEPERATED BY A COMMA>"
separator = "====================================================================="
# End of the User Editable Area
system_name = os.environ["SAPSYSTEMNAME"]
subject = "Table consistency report for {ph_system_name} on {ph_today}".format(ph_system_name=system_name, ph_today=today)
consistency_check_result_count = "SELECT COUNT(*) FROM M_CONSISTENCY_CHECK_HISTORY_ERRORS WHERE SEVERITY = 'HIGH';"
consistency_check_error_count = "SELECT COUNT(*) FROM M_CONSISTENCY_CHECK_HISTORY_ERRORS WHERE SEVERITY = 'ERROR';"
consistency_check_last_run = "SELECT TOP 1 * FROM (SELECT TO_VARCHAR(LAST_START_TIME, 'DD-MM-YYYY HH24:MI:SS') FROM M_CONSISTENCY_CHECK_HISTORY ORDER BY LAST_START_TIME DESC);"

# Create  a trace file and write any exceptions that have been caught to the trace file
try:
    if not os.path.exists("TableConsistencyCheckTraces"):
        os.makedirs("TableConsistencyCheckTraces")
    trace_file = open("TableConsistencyCheckTraces/Table_consistency_check_{ph_system_name}_{ph_today}.trc".format(ph_system_name=system_name, ph_today=today), "w+")
except Exception  as e:
    print("{ph_now} Unable to open or write to the trace file. Exception information:\n %s".format(ph_now=now) % e)

# Initialize the connection to HANA and execute the three queries defined at the start of this script
try:
    conn = dbapi.connect(
            key=hdbuserstore_key
            )
except Exception as e:
    trace_file.write("{ph_now} Unable to connect to the database. Exception information:\n %s".format(ph_now=now) % e)
    trace_file.close()
    sys.exit(1)

try:
    cursor = conn.cursor()
    sql_command = consistency_check_result_count
    cursor.execute(sql_command)
    rows = cursor.fetchall()
    for row in rows:
        for col in row:
            table_inconsistency_count = ("%s" % col)
    cursor.close()
except Exception as e:
    trace_file.write("{ph_now} Unable to execute the consistency check result count query. Exception information:\n %s".format(ph_now=now) % e)

try:
    cursor = conn.cursor()
    sql_command = consistency_check_error_count
    cursor.execute(sql_command)
    rows = cursor.fetchall()
    for row in rows:
        for col in row:
            consistency_error_count = ("%s" % col)
    cursor.close()
except Exception as e:
    trace_file.write("{ph_now} Unable to execute the consistency check error count query. Exception information:\n %s".format(ph_now=now) % e)

try:
    cursor = conn.cursor()
    sql_command = consistency_check_last_run
    cursor.execute(sql_command)
    if cursor.description is None:
        consistency_last_run = "No recent last run found"
    else:
        rows = cursor.fetchall()
        for row in rows:
            for col in row:
                consistency_last_run = ("%s" % col)
    cursor.close()
except Exception as e:
    trace_file.write("{ph_now} Unable to execute the consistency check last run query. Exception information:\n %s".format(ph_now=now) % e)

# Message template
message = """The following information displays the latest consistency check results for {ph_system_name} on {ph_today}
\n
{ph_separator}\n
Number of tables with inconsistencies: {ph_table_inconsistency_count}\n
{ph_separator} \n
\n
{ph_separator}\n
Number of consistency checks that have ended with an error: {ph_consistency_error_count}\n
{ph_separator} \n
\n
{ph_separator}\n
Last consistency run: {ph_consistency_last_run}\n
{ph_separator} \n
\n

Kind regards \n
""".format(ph_separator=separator, ph_system_name=system_name, ph_table_inconsistency_count=table_inconsistency_count, ph_consistency_error_count=consistency_error_count, ph_consistency_last_run=consistency_last_run, ph_today=today)

# Put together the report email and send
email_sender = smtplib.SMTP(host=smtp_server, port=smtp_port)
msg = MIMEMultipart()
msg["From"] = email_from
msg["To"] = email_to
# msg["Cc"] = email_cc
msg["Subject"] = subject
msg.attach(MIMEText(message, "plain"))

try:
    email_sender.sendmail(email_from,email_to,msg.as_string())
except Exception as e:
    trace_file.write("{ph_now} Unable to send the email. Exception information:\n %s".format(ph_now=now) % e)

# Disconnect from the SMTP server and close the connection
email_sender.quit()

# Close the trace file that was opened previously for writing any exceptions caught
trace_file.close()
