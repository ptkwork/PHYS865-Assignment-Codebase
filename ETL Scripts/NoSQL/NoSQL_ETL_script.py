import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
import json
from password import # Import MySQL password securely
from datetime import datetime, date
from decimal import Decimal

def serialise_datetime(obj):
    """Convert datetime, date, and Decimal objects to serialisable formats."""
    if isinstance(obj, (datetime, date)):  # Handle datetime and date
        return obj.isoformat()
    elif isinstance(obj, Decimal):  # Handle Decimal
        return float(obj)  # Convert Decimal to float
    raise TypeError(f"Type {type(obj)} not serialisable")



# Database connection details
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_DB = ""

MONGO_DB = ""
MONGO_COLLECTION = "Assessments"

def main():
    try:
        # Step 1: Connect to MySQL
        print("Connecting to MySQL...")
        mysql_conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=,
            database=MYSQL_DB
        )
        if mysql_conn.is_connected():
            print(f"Connected to MySQL database '{MYSQL_DB}'")

        # Step 2: Extract Data from MySQL
        cursor = mysql_conn.cursor(dictionary=True)
        sql_query = """
        SELECT 
            a.AssessmentID,
            a.PatientID,
            p.PIMSNumber,
            a.DateEntered,
            a.DateOfAssessment,
            a.ReferredBy,
            a.CareInvolvementDetails,
            hs.HospitalStatus,
            w.WoundID,
            w.WoundSiteOther,
            w.Lorr,
            w.WoundCategory,
            w.SizeLength,
            w.SizeWidth,
            w.SizeDepth,
            w.StateOfWoundBed,
            w.StateOfWoundBedOther,
            w.SurroundingSkin,
            w.SurroundingSkinOther,
            w.SuspectedCause,
            w.SuspectedCauseOther,
            w.ContributingFactor,
            w.ContributingFactorsOther,
            w.FrequencyOfDressing,
            w.CurrentDressingRegimen,
            ws.WoundSite,
            asr.AssessorID,
            asr.AssessorName,
            eng.EngineerID,
            eng.EngineerName
        FROM 
            Assessments a
        LEFT JOIN 
            Patients p ON a.PatientID = p.PatientID
        LEFT JOIN 
            HospitalAdmissionStatuses hs ON a.HospitalStatusID = hs.HospitalStatusID
        LEFT JOIN 
            Wounds w ON a.AssessmentID = w.AssessmentID
        LEFT JOIN 
            WoundSites ws ON w.WoundSiteID = ws.WoundSiteID
        LEFT JOIN 
            AssessmentAssessors aa ON a.AssessmentID = aa.AssessmentID
        LEFT JOIN 
            Assessors asr ON aa.AssessorID = asr.AssessorID
        LEFT JOIN 
            AssessmentEngineers ae ON a.AssessmentID = ae.AssessmentID
        LEFT JOIN 
            Engineers eng ON ae.EngineerID = eng.EngineerID
        """
        cursor.execute(sql_query)
        results = cursor.fetchall()
        print(f"Fetched {len(results)} records from MySQL.")

        # Ensure each field is serialisable and there are no circular references
        transformed_data = []
        for row in results:
            assessment = {
                "AssessmentID": row["AssessmentID"],
                "Patient": {
                    "PatientID": row["PatientID"],
                    "PIMSNumber": row["PIMSNumber"]
                },
                "DateEntered": row["DateEntered"],  # Serialised using serialise_datetime
                "DateOfAssessment": row["DateOfAssessment"],
                "ReferredBy": row["ReferredBy"],
                "CareInvolvementDetails": row["CareInvolvementDetails"],
                "HospitalStatus": {
                    "Status": row["HospitalStatus"]
                },
                "Wounds": [
                    {
                        "WoundID": row["WoundID"],
                        "WoundSite": row["WoundSite"],
                        "WoundSiteOther": row["WoundSiteOther"],
                        "Lorr": row["Lorr"],
                        "WoundCategory": row["WoundCategory"],
                        "Size": {
                            "Length": row["SizeLength"],
                            "Width": row["SizeWidth"],
                            "Depth": row["SizeDepth"]
                        },
                        "StateOfWoundBed": row["StateOfWoundBed"],
                        "StateOfWoundBedOther": row["StateOfWoundBedOther"],
                        "SurroundingSkin": row["SurroundingSkin"],
                        "SurroundingSkinOther": row["SurroundingSkinOther"],
                        "SuspectedCause": row["SuspectedCause"],
                        "SuspectedCauseOther": row["SuspectedCauseOther"],
                        "ContributingFactor": row["ContributingFactor"],
                        "ContributingFactorsOther": row["ContributingFactorsOther"],
                        "FrequencyOfDressing": row["FrequencyOfDressing"],
                        "CurrentDressingRegimen": row["CurrentDressingRegimen"]
                    }
                ],
                "Assessors": [
                    {"AssessorID": row["AssessorID"], "AssessorName": row["AssessorName"]}
                ],
                "Engineers": [
                    {"EngineerID": row["EngineerID"], "EngineerName": row["EngineerName"]}
                ]
            }
            
            # Safeguard: Ensure serialisability and remove unexpected structures
            assessment = json.loads(json.dumps(assessment, default=serialise_datetime))
            transformed_data.append(assessment)



        # Step 4: Write to JSON
        print("Writing data to transformed_2.0.json...")
        with open("transformed_2.0.json", "w") as json_file:
            json.dump(transformed_data, json_file, indent=4, default=serialise_datetime)
        print("Data written to transformed_2.0.json.")



        # Step 5: Load Data into MongoDB
        print("Connecting to MongoDB...")
        mongo_client = MongoClient("mongodb://localhost:27017/")
        mongo_db = mongo_client[MONGO_DB]
        mongo_collection = mongo_db[MONGO_COLLECTION]

        print("Inserting data into MongoDB...")
        mongo_collection.insert_many(transformed_data)
        print(f"Inserted {len(transformed_data)} documents into MongoDB.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        # Cleanup
        if mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    main()
