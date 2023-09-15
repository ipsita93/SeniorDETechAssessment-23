''' 
Purpose: Main Spark script to perform ETL on 2 datasets 
'''

import os
import pandas as pd

# Input and output directories
input_dir = "input"
output_dir = "output"
success_dir = os.path.join(output_dir, "apps_successful")
failure_dir = os.path.join(output_dir, "apps_unsuccessful")

# Process each CSV file in the input directory
for filename in os.listdir(input_dir):
    if filename.endswith(".csv"):
        input_path = os.path.join(input_dir, filename)
        df = pd.read_csv(input_path)
        print(df)
