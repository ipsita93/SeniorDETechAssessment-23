''' 
Purpose: Main Spark script to perform ETL on the input CSV files
Result: Stored in output directory categorised by applications success / failure 
'''

import os
import pandas as pd
import logging
import re
from datetime import datetime as pydt
import utility
import numpy as np

# Create a custom logger
logging.basicConfig(level = logging.INFO) # Change logging level to DEBUG, WARNING or INFO
logger = logging.getLogger(__name__)

# Takes in a dataframe
# Returns a clean dataframe
def cleanData(df): 
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
        logger.debug("[{}] has non alphabet chars in first part [{}] of name: {}".format(name, first_part, matches))
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
    # Convert dob to numpy datetime64 data type
    logger.debug("Convert dob to numpy datetime64 data type")
    dob_converted = utility.convert_to_datetime(date_of_birth)
    
    # Convert datetime64 data type to string in YYYYMMDD format
    dob_converted = dob_converted.strftime('%Y%m%d')
    logger.debug("Convert dob to YYYYMMDD: {}".format(dob_converted))

    return dob_converted

# Takes in date_of_birth string in YYYYMMDD format
# Returns a boolean which is true if the applicant is above 18 years as of 1 Jan 2022
def is_above_18(date_of_birth): 
    current_date = pydt(2022, 1, 1)
    date_of_birth = pydt.strptime(date_of_birth, '%Y%m%d')
    time_difference = current_date - date_of_birth
    age = round(time_difference.days/365)

    if age > 18: 
        logger.debug("Applicant is above 18 and age is [{}]".format(age))   
        return True
    else: 
        logger.debug("Applicant is below 18 and age is [{}]".format(age))   
        return False

# Takes in email string 
# Returns a boolean which is true if the applicant email ends with @emailprovider.com or @emailprovider.net
def is_valid_email(email): 
    # Regex to check valid email suffix according to requirement
    regex = "((?!-)[A-Za-z0-9-]" + "{1,63}(?<!-)\\.)" + "+(?:com|net)$"
    match = re.search(regex, email)

    if match != None: 
        logger.debug("[{}] is valid email".format(email))
        return True
    else: 
        logger.debug("[{}] is invalid email".format(email))
        return False

# Takes in name string 
# Returns a boolean which is true if the applicant's name is not empty
def has_no_name(name): 
    if name == None:
        logger.debug("No name: [{}]".format(name)) 
        return True
    else: 
        logger.debug("Has name: [{}]".format(name)) 
        return False

# Takes in mobile_no string  
# Returns a boolean which is true if the applicant's mobile no has 8 digits 
def is_valid_mobile_no(mobile_no): 
    if len(mobile_no) == 8:
        logger.debug("Mobile has 8 digits: [{}]".format(mobile_no)) 
        return True
    else: 
        logger.debug("Mobile does not have 8 digits: [{}]".format(mobile_no)) 
        return False

# Takes in a dataframe
# Returns a processed dataframe
def processData(df): 
    # print(df)

    # Split name into first_name and last_name
    logger.info("Split name")
    df['split_name'] = df['name'].apply( lambda x: split_name(x) )
    df['first_name'], df['last_name'] = zip(*df.split_name)

    # Format birthday field into YYYYMMDD
    logger.info("Format birthday")
    df['date_of_birth_YYYYMMDD'] = df['date_of_birth'].apply( lambda x: format_birthday(x) )
    
    # Add new field above_18 based on the applicant's birthday
    logger.info("Check if age is above 18 and add above_18 field")
    df['above_18'] = df['date_of_birth_YYYYMMDD'].apply( lambda x: is_above_18(x) )

    # Applicant has a valid email (email ends with @emailprovider.com or @emailprovider.net)
    # Add new field is_valid_email based on email
    logger.info("Check if email is valid and add is_valid_email field")
    df['is_valid_email'] = df['email'].apply( lambda x: is_valid_email(x) )

    # Remove any rows which do not have a name field (treat this as unsuccessful applications)
    # Add new field has_no_name based on name
    logger.info("Check if row has no name field and add has_no_name field")
    df['has_no_name'] = df['name'].apply( lambda x: has_no_name(x) )

    # Application mobile number is 8 digits
    # Add new field is_valid_mobile_no based on mobile_no
    logger.info("Check if mobile no is valid and add is_valid_mobile_no field")
    df['is_valid_mobile_no'] = df['mobile_no'].apply( lambda x: is_valid_mobile_no(x) )

    # Add field is_successful to categorize application as successful or unsuccessful
    logger.info("Check is successful applicant and add is_successful field")
    df['is_successful_applicant'] = df.apply( 
        lambda row: row['is_valid_mobile_no'] and row['above_18'] 
        and row['is_valid_email'] and row['has_no_name'] != True, axis=1 )

    # Membership IDs for successful applications should be the user's last name, followed by a SHA256 hash of the applicant's birthday, truncated to first 5 digits of hash (i.e <last_name>_<hash(YYYYMMDD)>)
    # Add new field membership_id based on last_name, date_of_birth_YYYYMMDD
    logger.info("Add membership_id field")
    df['membership_id'] = df.apply( 
        lambda row: row['last_name'] + '_' + 
        utility.calculate_hash(row['date_of_birth_YYYYMMDD'])[0:5] 
        if row['is_successful_applicant'] == True else None, axis=1 )
    
    # print(df)
    return df

# Takes in dataframe
# Returns one dataframe of successful applicants and the other of unsuccessful applicants
def splitDatabyApplicationSuccess(df): 
    df_successful = df[df['is_successful_applicant'] == True]
    # Select subset of columns for successful applications
    df_successful = df_successful[['first_name', 'last_name', 'email', 'date_of_birth_YYYYMMDD', 'mobile_no', 'above_18', 'membership_id']]
    
    df_unsuccessful = df[df['is_successful_applicant'] == False]
    # Select original and new columns for unsuccessful applications
    df_unsuccessful = df_unsuccessful[['name', 'email', 'date_of_birth', 'mobile_no', 'first_name', 'last_name', 'date_of_birth_YYYYMMDD', 'above_18']]
    
    return df_successful, df_unsuccessful


# Takes in input CSV filename
# Returns processed dataframes for successful and unsuccessful applications
def processCSV(filename): 
    if filename.endswith(".csv") and filename.startswith("applications_dataset_"):
        input_path = os.path.join(input_dir, filename)
        df = pd.read_csv(input_path)

        # Data cleaning
        logger.info("Cleaning data...")
        df = cleanData(df)

        # Data processing 
        logger.info("Processing data...")
        df = processData(df)
        logger.info("Split data into successful and unsuccessful applications")
        df_successful, df_unsuccessful = splitDatabyApplicationSuccess(df)

        return df_successful, df_unsuccessful


# Takes in input directory of all CSV paths and output directory for success and failure
# Returns consolidated and processed dataframes for successful and unsuccessful applications
# Writes successful and unsuccessful applications in output directory 
def main (input_dir, success_dir, failure_dir):
    # Store successful and unsuccessful application records for each dataset processed
    df_arr_successful = []
    df_arr_unsuccessful = []
    execution_date_hour = pydt.now().strftime("%Y%m%d_%H")

    # Process each CSV file in the input directory
    for filename in os.listdir(input_dir): 
        logger.info("Processing CSV: {}".format(filename))
        df_successful, df_unsuccessful = processCSV(filename)
        
        
        # Add processed data into a separate array
        df_arr_successful.append(df_successful)
        df_arr_unsuccessful.append(df_unsuccessful)    

    logger.info("Done processing CSVs: {}".format(filename))

    # Merge dataframes
    df_merged_successful = pd.concat(df_arr_successful)
    df_merged_unsuccessful = pd.concat(df_arr_unsuccessful)

    # Create output directories if they don't exist
    os.makedirs(success_dir, exist_ok=True)
    os.makedirs(failure_dir, exist_ok=True)
        
    # Write CSV outputs
    print(execution_date_hour)
    successful_output_path = success_dir + "/applications_{}.csv".format(execution_date_hour)
    unsuccessful_output_path = failure_dir + "/applications_{}.csv".format(execution_date_hour)
    
    logger.info("Writing output CSVs to paths: [{}] and [{}]".format(successful_output_path, unsuccessful_output_path))
    # Save data to output directories
    df_successful.to_csv(successful_output_path, index=False)
    df_unsuccessful.to_csv(unsuccessful_output_path, index=False)

if __name__ == "__main__":  # Input and output directories
    input_dir = "input"
    output_dir = "output"
    success_dir = os.path.join(output_dir, "applications_successful")
    failure_dir = os.path.join(output_dir, "applications_unsuccessful")
    
    main(input_dir, success_dir, failure_dir)