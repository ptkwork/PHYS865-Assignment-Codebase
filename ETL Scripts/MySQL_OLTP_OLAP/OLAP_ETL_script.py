import mysql.connector
import pandas as pd
from password import # Import password securely

# ETL Function
def run_etl():
    try:
        # Connect to OLTP and OLAP databases
        oltp_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=,
            database=""
        )
        olap_conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password=,
            database=""
        )

        oltp_cursor = oltp_conn.cursor(dictionary=True)
        olap_cursor = olap_conn.cursor()

        # Start transaction for OLAP
        olap_conn.start_transaction()

        # Extract data from OLTP
        # 1. DimPatients
        oltp_cursor.execute("SELECT DISTINCT PatientID, PIMSNumber FROM Patients;")
        dim_patients = oltp_cursor.fetchall()

        # 2. DimDates
        oltp_cursor.execute("SELECT DISTINCT DateOfAssessment FROM Assessments WHERE DateOfAssessment IS NOT NULL;")
        dates = pd.DataFrame(oltp_cursor.fetchall())

        # Transform dates to DimDates structure
        dates['Year'] = pd.to_datetime(dates['DateOfAssessment']).dt.year
        dates['Month'] = pd.to_datetime(dates['DateOfAssessment']).dt.month
        dates['Day'] = pd.to_datetime(dates['DateOfAssessment']).dt.day
        dates['Week'] = pd.to_datetime(dates['DateOfAssessment']).dt.isocalendar().week

        # 3. DimAssessors
        oltp_cursor.execute("SELECT DISTINCT AssessorID, AssessorName FROM Assessors;")
        dim_assessors = oltp_cursor.fetchall()

        # 4. DimEngineers
        oltp_cursor.execute("SELECT DISTINCT EngineerID, EngineerName FROM Engineers;")
        dim_engineers = oltp_cursor.fetchall()

        # 5. DimWounds
        oltp_cursor.execute("""
            SELECT DISTINCT 
                w.WoundID,
                ws.WoundSite,
                w.WoundCategory,
                w.SizeLength,
                w.SizeWidth,
                w.SizeDepth,
                w.AssessmentID
            FROM PUPIS3.Wounds w
            LEFT JOIN PUPIS3.WoundSites ws ON w.WoundSiteID = ws.WoundSiteID;
        """)
        dim_wounds = oltp_cursor.fetchall()


        # 6. FactAssessments
        oltp_cursor.execute("""
            SELECT 
                a.AssessmentID,
                a.PatientID,
                a.DateOfAssessment,
                COUNT(w.WoundID) AS WoundCount
            FROM Assessments a
            LEFT JOIN Wounds w ON a.AssessmentID = w.AssessmentID
            GROUP BY a.AssessmentID;
        """)
        fact_assessments = oltp_cursor.fetchall()

        # Load data into OLAP schema
        # 1. DimPatients
        olap_cursor.executemany("""
            INSERT IGNORE INTO DimPatients (PatientID, PIMSNumber)
            VALUES (%s, %s);
        """, [(row['PatientID'], row['PIMSNumber']) for row in dim_patients])

        # 2. DimDates
        for _, row in dates.iterrows():
            olap_cursor.execute("""
                INSERT IGNORE INTO DimDates (DateKey, Year, Month, Day, Week)
                VALUES (%s, %s, %s, %s, %s);
            """, (row['DateOfAssessment'], row['Year'], row['Month'], row['Day'], row['Week']))

        # 3. DimAssessors
        olap_cursor.executemany("""
            INSERT IGNORE INTO DimAssessors (AssessorID, AssessorName)
            VALUES (%s, %s);
        """, [(row['AssessorID'], row['AssessorName']) for row in dim_assessors])

        # 4. DimEngineers
        olap_cursor.executemany("""
            INSERT IGNORE INTO DimEngineers (EngineerID, EngineerName)
            VALUES (%s, %s);
        """, [(row['EngineerID'], row['EngineerName']) for row in dim_engineers])

        # 5. DimWounds
        olap_cursor.executemany("""
            INSERT IGNORE INTO DimWounds (WoundID, WoundSite, WoundCategory, SizeLength, SizeWidth, SizeDepth, AssessmentID)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, [(row['WoundID'], row['WoundSite'], row['WoundCategory'], row['SizeLength'], 
               row['SizeWidth'], row['SizeDepth'], row['AssessmentID']) for row in dim_wounds])

        # 6. FactAssessments
        olap_cursor.executemany("""
            INSERT IGNORE INTO FactAssessments (AssessmentID, PatientID, DateOfAssessment, WoundCount)
            VALUES (%s, %s, %s, %s);
        """, [(row['AssessmentID'], row['PatientID'], row['DateOfAssessment'], row['WoundCount']) for row in fact_assessments])

        # Commit transaction
        olap_conn.commit()
        print("ETL process completed successfully!")

    except Exception as e:
        print(f"Error occurred: {e}")
        olap_conn.rollback()  # Rollback in case of any errors
        print("Transaction rolled back due to an error.")

    finally:
        if oltp_conn.is_connected():
            oltp_cursor.close()
            oltp_conn.close()
        if olap_conn.is_connected():
            olap_cursor.close()
            olap_conn.close()
        print("Connections closed.")

# Run the ETL process
if __name__ == "__main__":
    run_etl()
