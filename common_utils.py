################################################################################
#                                                                              #
# Common utilities.                                                            #
#                                                                              #
################################################################################

# pandas.
from pandas import DataFrame, read_csv

# json.
from json import load

#!/usr/bin/env python
import os

# subprocess.
import subprocess

# requests.
import requests

# sparql_dataframe.
from sparql_dataframe import get

# const.
from const import ENDPOINT,TEST_QUERY


###################
#                 #
# Common methods. #
#                 #
###################

#
# csv_to_df(_f_path: string _dtype: dictionary): -> DataFrame.
#
def csv_to_df(_f_path: str, _dtype: dict) -> DataFrame:
    """
    Produce a pandas dataframe form csv file.

    Args:
        _f_path (string)
        _dtype (dict)

    Returns:
        (DataFrame)
    """
    
    try:
        data_frame = read_csv(
                        _f_path, 
                        keep_default_na = False, 
                        dtype           = _dtype, 
                        sep             = ',',
                        encoding        = 'utf-8'
                    )
        return data_frame

    except Exception:
        print('-- ERR: csv file is empty or malformed!')
        raise

 
#
# json_to_df(_f_path: string): -> DataFrame.
#
def json_to_df(_f_path: str) -> DataFrame:
    """
    Produce pandas dataframe from json.

    Args:
        _f_path (string)

    Returns:
        DataFrame:
    """
    
    try:
        with open(_f_path, 'r', encoding='utf-8') as f:
            data_frame = load(f)    
            
            return data_frame

    except Exception:
        print('-- ERR: json file is empty or malformed!')
        raise   

################################
#                              #
# Create foldern if not exist. #
#                              #
################################
def create_folder(_folderName: str) -> None:
    
    if isinstance(_folderName, str):    
        if not os.path.exists(_folderName):
            os.makedirs(_folderName)
    else:
        raise("-- ERR: _folderName is not a string!")

###############################################
#                                             #
# Common methods to interact with Blazegraph. #
#                                             #
###############################################

#
# Check if the blazegraph insance is already active.
#
def blazegraph_instance_is_active() -> bool:
    try:
        page = requests.get('http://127.0.0.1:9999/blazegraph/#splash')
        
        if page.status_code == 200 :
            
            return True
        else:
            return False

    except Exception as error:

        print(error.args)

        return False


#
# Check if there is something in blazegraph db yet.
#
def blazegraph_instance_is_empty() -> bool:
        
    df_sparql = get(ENDPOINT, TEST_QUERY, True)
    
    if df_sparql.empty:
        return True
    else:
        return False # TEST: set True only for test! Otherwise False


#
# Start the blazegraph server db instance.
#
def start_blazegraph_server() -> None:
    try: 
        #os.system("start \"\" cmd /c \"cd D:\\Sviluppo\\git-repo\\projects\\data-science-uni-bo\\allorapy-data-science\\blazegraph && java -server -Xmx4g -jar blazegraph.jar\"")
        
        # Join the current directory path with the target folder.
        blazegraph_path = os.path.join(os.path.dirname(__file__), './blazegraph' )

        subprocess.Popen(
            'java -server -Xmx4g -jar blazegraph.jar', 
            cwd    = blazegraph_path,
            shell  = True,
        )

    except Exception:
        raise