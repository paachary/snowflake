use role accountadmin;

-- create a new database for this exercise

create or replace database dataload_db;

-- create a new schema 
use database dataload_db;

create or replace schema dataload;

-- Create a separate ROLE and USER for executing snowpipe

use role accountadmin;

CREATE OR REPLACE ROLE developer ;

grant role DEVELOPER to role SYSADMIN;

CREATE USER dev_1 PASSWORD = '*****' LOGIN_NAME = dev_1 DEFAULT_ROLE = developer
DEFAULT_WAREHOUSE = 'compute_wh' MUST_CHANGE_PASSWORD = false;

GRANT ROLE developer to USER dev_1;

-- login as an admin and create a file format and stage in the schema created above.

use schema dataload;

CREATE OR REPLACE FILE FORMAT csv_format TYPE='CSV'
    RECORD_DELIMITER='\\n'
    FIELD_DELIMITER='|'
    TRIM_SPACE=FALSE
    NULL_IF = ('NULL','null','','          ');

CREATE OR REPLACE STAGE internal_stg file_format = csv_format;

PUT file:///home/hadoop/snowflake/departments.csv @internal_stg;

CREATE OR REPLACE TABLE departments
(department_id INT, 
 department_name STRING, 
 manager_id INT, 
 location_id INT );
 
 -- create a pipe to load data from stage to the target table
 
 CREATE OR REPLACE PIPE load_dept_pipe AS
 COPY INTO departments from @internal_stg;
 
ALTER PIPE LOAD_DEPT_PIPE SET PIPE_EXECUTION_PAUSED=true;

-- grant the required privileges for the user to execute snowpipe.

GRANT OWNERSHIP ON pipe load_dept_pipe to role developer;
GRANT USAGE ON file format csv_format TO ROLE developer;
GRANT READ on STAGE internal_stg to role developer;
GRANT USAGE ON DATABASE dataload_db to role developer;
GRANT USAGE ON SCHEMA dataload to role developer;
GRANT SELECT , INSERT ON departments to role developer;

select SYSTEM$PIPE_FORCE_RESUME('LOAD_DEPT_PIPE');

SELECT SYSTEM$PIPE_STATUS('LOAD_DEPT_PIPE');

-- Get the environment ready for creating rest API in python
/*

// Install the python sdk for snowflake
pip install  snowflake-ingest

// configure security (per user)

1. generate the private key with encrypted version (passphrase)

openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8

2. generate the public key

openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

3. copy the private key and public key to a local storage directory

4. assign the public key to the above created snowflake database user
*/

alter user dev_1 set rsa_public_key='<Paste the public key from the file. Do not copy the header portion of the public key from the file>';

-- describe the user and verify the public key 
DESCRIBE USER dev_1;

/*

export PRIVATE_KEY_PASSPHRASE='<passphrase>'

Write the sample python code to load the data continually into the departments table using the pipe.

from logging import getLogger
from snowflake.ingest import SimpleIngestManager
from snowflake.ingest import StagedFile
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import time
import datetime
import os
import logging

logging.basicConfig(
        filename='/tmp/ingest.log',
        level=logging.DEBUG)
logger = getLogger(__name__)

with open("/<private_key_path>/rsa_key.p8", 'rb') as pem_in:
  pemlines = pem_in.read()
  private_key_obj = load_pem_private_key(pemlines,
  os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
  default_backend())

private_key_text = private_key_obj.private_bytes(
  Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')
# Assume the public key has been registered in Snowflake:
# private key in PEM format

# List of files in the stage specified in the pipe definition
file_list=['<path>/<filename>', '<path>/<filename>']
ingest_manager = SimpleIngestManager(account='<account_name>',
                                     host='<account_name>.<region_id>.snowflakecomputing.com',
                                     user='<user_login_name>',
                                     pipe='<db_name>.<schema_name>.<pipe_name>',
                                     private_key=private_key_text)
# List of files, but wrapped into a class
staged_file_list = []
for file_name in file_list:
    staged_file_list.append(StagedFile(file_name, None))

try:
    resp = ingest_manager.ingest_files(staged_file_list)
except HTTPError as e:
    # HTTP error, may need to retry
    logger.error(e)
    exit(1)

# This means Snowflake has received file and will start loading
assert(resp['responseCode'] == 'SUCCESS')

# Needs to wait for a while to get result in history
while True:
    history_resp = ingest_manager.get_history()

    if len(history_resp['files']) > 0:
        print('Ingest Report:\n')
        print(history_resp)
        break
    else:
        # wait for 20 seconds
        time.sleep(20)

    hour = timedelta(hours=1)
    date = datetime.datetime.utcnow() - hour
    history_range_resp = ingest_manager.get_history_range(date.isoformat() + 'Z')

    print('\nHistory scan report: \n')
    print(history_range_resp)


*/


USE ROLE ACCOUNTADMIN;

DROP DATABASE dataload_db;
