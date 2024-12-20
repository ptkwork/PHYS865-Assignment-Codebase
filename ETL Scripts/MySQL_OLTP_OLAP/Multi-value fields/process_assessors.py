import pandas as pd

# Load exported data
df = pd.read_csv('assessor.csv')

# Clean the data
# 1. Remove leading/trailing spaces
df['AssessorName'] = df['AssessorName'].str.strip()

# 2. Remove internal spaces
df['AssessorName'] = df['AssessorName'].str.replace(' ', '', regex=False)

# 3. Convert to uppercase
df['AssessorName'] = df['AssessorName'].str.upper()

# Split AssessorName into multiple rows
split_data = (
    df.assign(AssessorName=df['AssessorName'].str.split(','))
      .explode('AssessorName')
      .reset_index(drop=True)
)

# Save the processed data to a new file
split_data.to_csv('split_assessors.csv', index=False)

print("Data cleaned, split, and saved successfully!")
