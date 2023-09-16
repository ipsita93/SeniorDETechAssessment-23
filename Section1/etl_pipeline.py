''' 
Purpose: Main Spark script to perform ETL on the input CSV files
Result: Stored in output directory categorised by applications success / failure 
'''

import os
import pandas as pd
import logging
import re
from datetime import datetime
import utility
import numpy as np

# Create a custom logger
logging.basicConfig(level = logging.DEBUG) # Change logging level to DEBUG, WARNING or INFO
logger = logging.getLogger(__name__)

# Takes in a dataframe
# Returns a clean dataframe
def cleanData(df): 
    logger.info("Cleaning data...")

    # Strip spaces in values of every field
    logger.info("Strip spaces in all fields")
    for column in df:
        df[column] = df[column].str.strip()
   
    # Convert Null or NaN names to None
    logger.info("Convert Null or NaN names to None")
    df['name'].where(pd.notnull(df['name']), None)

    return df

# Checks if first part of name has non-alphabet char suggesting name prefix
# Like Mr., Dr., etc
def has_non_alpha_in_first_part(name):
    first_part = name.split(' ')[0]
    matches = re.findall("[^A-Za-z ]",first_part)
    res = len(matches) > 0
    if res:
        logger.warning("[{}] has non alphabet chars in first part [{}] of name: {}".format(name, first_part, matches))
    return res

# Split name with prefix
# Returns tuple
def split_name_with_prefix(name):
    # Split and get everything within first 2 spaces for first name, rest is last name
    split_name = name.split(" ")
    # To remove prefix like Mr. Mrs. Dr. etc, use split_name[1] for first name
    return (split_name[1], split_name[-1])

# Split name without prefix
# Returns tuple
def split_name_without_prefix(name):
    # Split and get everything within first 1 space for first name, rest is last name
    split_name = name.split(" ")
    return (split_name[0], split_name[-1])

# Takes in a name string 
# Returns the split name into tuple (first_name, last_name)
def split_name(name): 
    # If empty name, return empty tuple 
    if name == None:
        return (None, None)
    
    if has_non_alpha_in_first_part(name) == True:
        res = split_name_with_prefix(name)
        logger.debug("Has prefix - from: [{}] to: {}".format(name, res))
        return res
    else:
        res = split_name_without_prefix(name)
        logger.debug("No prefix - from: [{}] to: {}".format(name, res))
        return res

# Takes in date_of_birth string
# Returns the birthday in YYYYMMDD format 
def format_birthday(date_of_birth): 
    print(date_of_birth)

    # Convert dob to numpy datetime64 data type
    logger.info("Convert dob to numpy datetime64 data type")
    dob_converted = utility.convert_to_datetime(date_of_birth)

    # Convert datetime64 data type to string in YYYYMMDD format
    logger.info("Convert dob to YYYYMMDD")
    dob_converted = dob_converted.strftime('%Y%m%d')

    return dob_converted

# Takes in a dataframe
# Returns a processed dataframe
def processData(df): 
    logger.info("Processing data...")
    print(df)

    # Split name into first_name and last_name
    logger.info("Split name")
    df['split_name'] = df['name'].apply( lambda x: split_name(x) )
    df['first_name'], df['last_name'] = zip(*df.split_name)

    # Format birthday field into YYYYMMDD
    logger.info("Format birthday")
    df['date_of_birth_YYYYMMDD'] = df['date_of_birth'].apply( lambda x: format_birthday(x) )
    
    # # Create a new field named above_18 based on the applicant's birthday
    # logger.info("Add above_18 field")
    # df['above_18'] = df['date_of_birth_YYYYMMDD'].apply( lambda x: is_above_18(x) )

    # Add field to categorize application as successful or unsuccessful 
    # Application mobile number is 8 digits
    # Applicant is over 18 years old as of 1 Jan 2022
    # Applicant has a valid email (email ends with @emailprovider.com or @emailprovider.net)
    # Remove any rows which do not have a name field (treat this as unsuccessful applications)
    # Membership IDs for successful applications should be the user's last name, followed by a SHA256 hash of the applicant's birthday, truncated to first 5 digits of hash (i.e <last_name>_<hash(YYYYMMDD)>)

    print(df)
    
# Takes in input CSV filename
# Returns processed dataframes for successful and unsuccessful applications
def processCSV(filename): 
    if filename.endswith(".csv") and filename.startswith("applications_dataset_"):
        input_path = os.path.join(input_dir, filename)
        df = pd.read_csv(input_path)

        # Data cleaning
        df = cleanData(df)
        # Data processing 
        df = processData(df)

# Takes in input directory of all CSV paths and output directory for success and failure
# Returns consolidated and processed dataframes for successful and unsuccessful applications
# Writes successful and unsuccessful applications in output directory 
def main (input_dir, success_dir, failure_dir):
    # Process each CSV file in the input directory
    for filename in os.listdir(input_dir): 
        logger.info("Processing CSV: {}".format(filename))
        processCSV(filename)

if __name__ == "__main__":
    # Input and output directories
    input_dir = "input"
    output_dir = "output"
    success_dir = os.path.join(output_dir, "applications_successful")
    failure_dir = os.path.join(output_dir, "applications_unsuccessful")
    
    # Create output directories if they don't exist
    os.makedirs(success_dir, exist_ok=True)
    os.makedirs(failure_dir, exist_ok=True)

    main(input_dir, success_dir, failure_dir)