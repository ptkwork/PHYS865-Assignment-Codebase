import mysql.connector
import pandas as pd

# Connect to the database
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database=""
)

# Query the data
query = """
SELECT a.AssessmentID, asr.AssessorName
FROM Assessments a
JOIN Assessors asr ON a.AssessorID = asr.AssessorID;
"""
data = pd.read_sql(query, connection)

# Save to a CSV file
data.to_csv('assessor.csv', index=False)

# Close the connection
connection.close()
