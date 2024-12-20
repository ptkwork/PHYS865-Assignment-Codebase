import mysql.connector
import pandas as pd

def connect_to_database():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database=""
    )

def clean_data(row):
    """Clean and transform data based on specified rules."""
    # VARCHAR fields with specified lengths (truncates long strings)
    row['AssessorsInAttendance'] = row['assessorsinattend'][:100] if pd.notnull(row['assessorsinattend']) else "NR"
    row['RehabEngineerInAttendance'] = row['rehabenginattend'][:100] if pd.notnull(row['rehabenginattend']) else "NR"
    row['ReferredBy'] = row['referredby'][:100] if pd.notnull(row['referredby']) else "NR"
    row['CareInvolvementDetails'] = row['careinvol'][:100] if pd.notnull(row['careinvol']) else "NR"
    row['HospitalAdmissionStatus'] = row['hospadmin_dt_pu'][:10] if pd.notnull(row['hospadmin_dt_pu']) else "NR"
    row['WoundSite'] = row['wndsite1'][:50] if pd.notnull(row['wndsite1']) else None
    row['WoundSiteOther'] = row['wndsite1_other'][:100] if pd.notnull(row['wndsite1_other']) else None
    row['Lorr'] = row['lorr1'][:50] if pd.notnull(row['lorr1']) else None
    row['WoundCategory'] = row['cat1'][:50] if pd.notnull(row['cat1']) else None
    row['StateOfWoundBed'] = row['stateofwndbed1'][:100] if pd.notnull(row['stateofwndbed1']) else None
    row['StateOfWoundBedOther'] = row['stateofwndbed1_other'][:200] if pd.notnull(row['stateofwndbed1_other']) else None
    row['SurroundingSkin'] = row['surroundingskin1'][:100] if pd.notnull(row['surroundingskin1']) else None
    row['SurroundingSkinOther'] = row['surroundingskin1_other'][:200] if pd.notnull(row['surroundingskin1_other']) else None
    row['SuspectedCause'] = row['suspec_cause1'][:100] if pd.notnull(row['suspec_cause1']) else None
    row['SuspectedCauseOther'] = row['suspec_cause1_other'][:100] if pd.notnull(row['suspec_cause1_other']) else None
    row['ContributingFactor'] = row['contrib_fact1'][:100] if pd.notnull(row['contrib_fact1']) else None
    row['ContributingFactorsOther'] = row['contrib_factors1_other'][:100] if pd.notnull(row['contrib_factors1_other']) else None
    row['FrequencyOfDressing'] = row['freqofdress1'][:50] if pd.notnull(row['freqofdress1']) else None
    row['CurrentDressingRegimen'] = row['cur_dress_regime1'][:200] if pd.notnull(row['cur_dress_regime1']) else None
    
    # INT field for dtb_entry_no, using NULL for missing values
    row['dtb_entry_no'] = int(row['dtb_entry_no']) if pd.notnull(row['dtb_entry_no']) else None

    # DATE fields, setting NULL for missing values
    row['DateEntered'] = row['dateentered'] if pd.notnull(row['dateentered']) else None
    row['DateOfAssessment'] = row['dateofassess'] if pd.notnull(row['dateofassess']) else None

    # DECIMAL fields (SizeWidth, SizeLength, SizeDepth)
    row['SizeWidth'] = float(row['size_width1']) if pd.notnull(row['size_width1']) else None
    row['SizeLength'] = float(row['size_length1']) if pd.notnull(row['size_length1']) else None
    row['SizeDepth'] = float(row['size_depth1']) if pd.notnull(row['size_depth1']) else None
    
    return row

def insert_data(cursor, row):
    """Insert data into the database."""
    # Insert patient record (PatientID is auto-generated)
    cursor.execute("""
        INSERT INTO Patients (PIMSNumber) 
        VALUES (%s) 
        ON DUPLICATE KEY UPDATE PIMSNumber = PIMSNumber
    """, (row['pims_no'],))
    cursor.execute("SELECT PatientID FROM Patients WHERE PIMSNumber = %s", (row['pims_no'],))
    patient_id = cursor.fetchone()[0]

    # Insert hospital admission status (HospitalStatusID is auto-generated)
    cursor.execute("""
        INSERT INTO HospitalAdmissionStatuses (HospitalStatus) 
        VALUES (%s) 
        ON DUPLICATE KEY UPDATE HospitalStatus = HospitalStatus
    """, (row['HospitalAdmissionStatus'],))
    cursor.execute("SELECT HospitalStatusID FROM HospitalAdmissionStatuses WHERE HospitalStatus = %s", (row['HospitalAdmissionStatus'],))
    hospital_status_id = cursor.fetchone()[0]

    # Insert assessment record
    cursor.execute("""
        INSERT INTO Assessments (
            PatientID, DateEntered, DateOfAssessment, AssessorsInAttendance, 
            RehabEngineerInAttendance, ReferredBy, HospitalStatusID, CareInvolvementDetails
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        patient_id, row['DateEntered'], row['DateOfAssessment'], row['AssessorsInAttendance'], 
        row['RehabEngineerInAttendance'], row['ReferredBy'], hospital_status_id, row['CareInvolvementDetails']
    ))

    # Get AssessmentID for the newly inserted assessment
    assessment_id = cursor.lastrowid

    # Insert wound record
    cursor.execute("""
        INSERT INTO Wounds (
            AssessmentID, WoundSite, WoundSiteOther, Lorr, WoundCategory,
            SizeLength, SizeWidth, SizeDepth, StateOfWoundBed, StateOfWoundBedOther,
            SurroundingSkin, SurroundingSkinOther, SuspectedCause, SuspectedCauseOther,
            ContributingFactor, ContributingFactorsOther, FrequencyOfDressing, CurrentDressingRegimen
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        assessment_id, row['wndsite1'], row['wndsite1_other'], row['lorr1'], row['cat1'],
        row['SizeLength'], row['SizeWidth'], row['SizeDepth'], row['stateofwndbed1'], row['stateofwndbed1_other'],
        row['surroundingskin1'], row['surroundingskin1_other'], row['suspec_cause1'], row['suspec_cause1_other'],
        row['contrib_fact1'], row['contrib_factors1_other'], row['freqofdress1'], row['cur_dress_regime1']
    ))

def main():
    # Load data from the flat file
    filepath = ""
    df = pd.read_excel(filepath).replace("", None)  # Replace all empty strings in DataFrame with None
    df = df.where(pd.notnull(df), None)  # Replace NaN with None for MySQL compatibility

    # Connect to MySQL
    connection = connect_to_database()
    cursor = connection.cursor()

    try:
        for _, row in df.iterrows():
            # Skip rows without a PIMS number - essential for linking data
            if not row['pims_no']:
                continue
            
            # Clean data for each row
            cleaned_row = clean_data(row)

            # Insert cleaned data into the database
            insert_data(cursor, cleaned_row)

        # Commit the transaction after all inserts are successful
        connection.commit()
        print("Data loaded successfully!")

    except mysql.connector.Error as error:
        print(f"Error: {error}")
        connection.rollback()  # Rollback in case of an error

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")


if __name__ == "__main__":
    main()
