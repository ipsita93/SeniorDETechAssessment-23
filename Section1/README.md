# Section 1: Data Pipelines

## Structure

| File                      | Description                       |
| :-------------------------| :-------------------------------- |
| `automate_etl_dag.py`     | Airflow DAG.                      |
| `etl_pipeline.py`         | Main script that extracts, transforms and writes out the dataset into the output folder as CSVs. |
| `utility.py`         | Some helper functions used by main `etl_pipeline.py` python code. |

## Assumptions

### Name

- Name may have prefix and suffix such as Dr., Mr., Mrs. etc.
- First name should be prefix + actual first name if prefix exists.
- Otherwise, First name should be actual first part of name when prefix doesn't exists. 
- Last word in name should be last name. 
- Middle name is to be ignored. 

        e.g 1)  Kelly Smith DDS --> first_name=Kelly | last_name=DDS

        e.g 2) Mr. Bryan Porter --> first_name=Bryan | last_name=Porter

### Date of Birth 
- We assume that the date_of_birth field is in either YYYY/MM/DD or mm/dd/YYYY or dd/mm/YYYY formats only. Slashes(/) can be replaced by dashes(-) too. 

### Application Successful Logic
- We assume that the application successful logic is additive, i.e. for an applicant to be successful, all three statements below must be true. 
    - Application mobile number is 8 digits
    - Applicant is over 18 years old as of 1 Jan 2022
    - Applicant has a valid email (email ends with @emailprovider.com or @emailprovider.net)



## Airflow Scheduling 

To ensure that the pipeline runs hourly, we run the following commands to set up an Airflow DAG. 

In terminal, change directory to repository where dags are stored: 

`cd /Library/Frameworks/Python.framework/Versions/3.9/lib/python3.9/site-packages/airflow/example_dags`

Write the dag script as well as all other input and output directories and files (such as CSV input files)
e.g. `vi automate_etl_pipeline.py`

Run the airflow standalone: 
`airflow standalone`

Access 'http://localhost:8080/' to view the Airflow UI. 