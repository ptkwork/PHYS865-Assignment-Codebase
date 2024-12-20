import pandas as pd
import mysql.connector

# Load the CSV file
df = pd.read_csv('distinct_assessors.csv')

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database=""
)
cursor = connection.cursor()

# Insert the data into the Assessors table
for _, row in df.iterrows():
    cursor.execute("INSERT INTO Assessors (AssessorName) VALUES (%s)", (row['AssessorName'],))

# Commit and close the connection
connection.commit()
cursor.close()
connection.close()

print("Data successfully loaded into the Assessors table!")

