import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Database connection details
host = "localhost"
user = ""
password = ""
database = ""

# Connect to the database
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Query to fetch SummaryAssessmentsByYear data for years 2015 to 2024
query = """
SELECT 
    Year, 
    TotalAssessments
FROM 
    SummaryAssessmentsByYear
WHERE 
    Year BETWEEN 2015 AND 2024
ORDER BY 
    Year ASC;
"""

# Fetch data
data = pd.read_sql(query, conn)

# Close connection
conn.close()

# Plot the data as a bar chart
plt.figure(figsize=(12, 6))
bars = plt.bar(data['Year'], data['TotalAssessments'], color=plt.cm.tab20.colors)

# Add grid for better readability
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Titles and labels
plt.title('SummaryAssessmentsByYear', fontsize=16, fontweight='bold')
plt.xlabel('Year', fontsize=12)
plt.ylabel('Total Assessments', fontsize=12)
plt.xticks(data['Year'], rotation=45)
plt.yticks(fontsize=10)

# Add values on top of each bar
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width() / 2.0, height + 5, f'{int(height)}', ha='center', fontsize=10)

# Show the plot
plt.tight_layout()
plt.show()
