import pandas as pd

# Load the split assessors data
split_assessors = pd.read_csv('split_assessors.csv')

# Extract distinct AssessorName values
distinct_assessors = split_assessors[['AssessorName']].drop_duplicates()

# Save the distinct names to a new CSV file
distinct_assessors.to_csv('distinct_assessors.csv', index=False)

print("Distinct assessor names saved successfully!")
