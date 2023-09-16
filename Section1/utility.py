#!/usr/bin/env python3

import datetime
from datetime import datetime as pydt
import re

def convert_to_datetime (str):
    if not str:
        return None

    # DEPRICATED MONTH PATTERN: ([janfebmrpulgsoctvdJANFEBMRPULGSOCTVD]{3})

    # Change ddmmmyy to dd mmm yy
    if len(str) == 7:
        str = re.sub(r'([0-3][0-9])([jfmasondJFMASOND][aepucoAEPUCO][nbrylgptvcNBRYLGPTVC])([0-9]{2})', r'\1 \2 \3', str)

    # Change dmmmyy to d mmm yy
    elif len(str) == 6:
        str = re.sub(r'([1-9])([jfmasondJFMASOND][aepucoAEPUCO][nbrylgptvcNBRYLGPTVC])([0-9]{2})', r'\1 \2 \3', str)

    # Change ddmmmyyyy to dd mmm yyyy
    elif len(str) == 9:
        str = re.sub(r'([0-3][0-9])([jfmasondJFMASOND][aepucoAEPUCO][nbrylgptvcNBRYLGPTVC])([0-9]{4})', r'\1 \2 \3', str)
    
    # Change dmmmyyyy to d mmm yyyy
    elif len(str) == 8:
        str = re.sub(r'([1-9])([jfmasondJFMASOND][aepucoAEPUCO][nbrylgptvcNBRYLGPTVC])([0-9]{4})', r'\1 \2 \3', str)

    # # Define the format of the date string
    # date_format = "%Y-%m-%d"

    # try:
    #     # Parse the date string into a datetime object
    #     dates = pydt.strptime(str, date_format)
    #     # Display the parsed date object
    #     print(dates)
    # except ValueError:
    #     print("Invalid date format. Please use YYYY-MM-DD.")

    # for d in dates:
    #     if isinstance(d, datetime.datetime):
    #         return d

    # # Determine and convert yyyymmdd or ddmmyyyy
    # if len(str) != 8 or not str.isdigit():
    #     return None
    str = str.replace('-', '')  
    str = str.replace('/', '')  

    d = None
    try:
        print ("Trying to convert", str, "from yyyymmdd...")
        d = pydt.strptime(str, '%Y%m%d')
    except ValueError:
        try:
            print ("Trying to convert", str, "from yyyyddmm...")
            d = pydt.strptime(str, '%Y%m%d')
        except ValueError:
            try:
                print ("Trying to convert", str, "from ddmmyyyy...")
                d = pydt.strptime(str, '%d%m%Y')
            except ValueError:
                try:
                    print ("Trying to convert", str, "from mmddyyyy...")
                    d = pydt.strptime(str, '%m%d%Y')
                except ValueError:
                    print ("WARNING: Cannot convert", str, "to datetime.")
        
    return d


