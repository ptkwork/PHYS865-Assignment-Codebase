import pandas as pd
import mysql.connector

# Load the split_assessors.csv with AssessmentID and AssessorName
split_data = pd.read_csv('split_assessors.csv')

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database=""
)
cursor = connection.cursor()

# For each row in the split data, map AssessorName to AssessorID and insert into AssessmentAssessors
for _, row in split_data.iterrows():
    # Find the AssessorID for the given AssessorName
    cursor.execute("SELECT AssessorID FROM Assessors WHERE AssessorName = %s", (row['AssessorName'],))
    result = cursor.fetchone()
    if result:
        assessor_id = result[0]
        # Insert into AssessmentAssessors
        cursor.execute(
            "INSERT INTO AssessmentAssessors (AssessmentID, AssessorID) VALUES (%s, %s)",
            (row['AssessmentID'], assessor_id)
        )

# Commit changes and close the connection
connection.commit()
cursor.close()
connection.close()

print("Junction table AssessmentAssessors populated successfully!")
