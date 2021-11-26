The purpose of the TableConsistencyCheckReport.py script is to retrieve the latest consistency check results (the number of inconsistencies found, the number of errors reported during a consistency check, and the last consistency check run date and time) from a HANA tenant database, which is then placed into an email and sent to the specified recipients. 

The requirement was to reduce the manual effort required to frequently execute manual reviews of the consistency check results on multiple systems, and to automate the process instead. 

The line # email_cc = "LIST OF ADDRESSES SEPERATED BY A COMMA" can be uncommented if the report has to be sent to multiple recipients. The line # msg["Cc"] = email_cc should then also be uncommented so that the value(s) set for the email_cc variable is attached to the message.
