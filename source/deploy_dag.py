# Copyright 2024 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
import os
import json
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import argparse
import yaml
import logging
import google.auth
import google.auth.transport.requests
from jinja2 import Environment, FileSystemLoader
from google.cloud import storage

class validate_deploy_dag:

    def __init__(self,config):
        print(config)
        self.config = config
        self.project_id = config['gcp_project_id']
        self.location = config['composer_env_location']
        self.gcp_composer_env_name = config['gcp_composer_env_name']
        self.dynamic_dag_flag = config['dynamic_dag_flag']
        self.dag_file = config['dag_file']
        self.dynamic_dag_config_file = config['dynamic_dag_config_file']
    
    def google_api_headers(self):
        """ This function gets access tokens and authorizations\
            to access cloud healthcare API Fhir store"""
        creds, project = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        creds.refresh(auth_req)
        authToken = creds.token
        authHeaders = {
            "Authorization": f"Bearer {authToken}",
            "Prefer": "handling=strict"
            }
        return authHeaders
    
    def createRequestSession(self):
        """Creating request session to try GET/POST requests using below force list"""
        reqSession = requests.Session()
        retries = Retry(total=3,
                        backoff_factor=2,
                        status_forcelist=[429, 500, 502, 503, 504, 400, 404, 401])

        reqSession.mount('http://', HTTPAdapter(max_retries=retries))
        return reqSession

    def validate_file_path(self):
        """This function validates the file path for given dag and config_dag """
        """
        :param self
        :return str dag_filename: DAG filename
        :return str dag_directory_path: DAG local file path
        :return str dag_file_validation: DAG File validation flag Success or Fail
        :return str config_dag_filename: Dynamic Config DAG filename
        :return str config_dag_directory_path: Config DAG local file path
        :return str config_dag_validation: Config DAG File validation flag Success or Fail
        """
        try:
            logging.info("Reading dag_file and config_dag_file path to validate file path")
            dag_directory = str(self.dag_file).split("/")
            dag_directory_path = '/'.join(dag_directory[:-1])
            dag_filename = dag_directory[-1]
            dag_files = [ file for file in  os.listdir(dag_directory_path)]

            if dag_filename in dag_files:
                dag_file_validation = "success"
                logging.info("DAG file path successfully validated")
            else:
                dag_file_validation = "fail"
                logging.error(" Please check DAG filename or file path as file:{}\
                            not found at the given path {}".format(dag_filename,dag_directory_path))

            if self.dynamic_dag_flag == "True":
                config_dag_directory = str(self.dynamic_dag_config_file).split("/")
                config_dag_directory_path = '/'.join(config_dag_directory[:-1])
                config_dag_filename = config_dag_directory[-1]
                config_dag_files = [ file for file in  os.listdir(config_dag_directory_path)]
                if config_dag_filename in config_dag_files:
                    config_dag_validation = "success"
                    logging.info("Config DAG file path successfully validated")
                else:
                    config_dag_validation = "fail"
                    logging.warning(" Please check Dyamic DAG config filename or file path as file:{}\
                            not found at the given path {}".format(config_dag_filename,config_dag_directory))
            elif self.dynamic_dag_flag == "False":
                config_dag_validation = "pass"
                config_dag_filename = ""
                config_dag_directory_path = ""
                logging.warning(" Skipping to validate dynamic dag config file\
                    validation as dynamic_dag_flag value is False ")
            else:
                config_dag_validation = "fail"
                logging.warning(" Skipping to validate dynamic dag config file\
                    validation as expected dynamic_dag_flag value should be True or False")
                
        except Exception as error:
            logging.error(" File validation failed due to following errors : {}".format(str(type(error).__name__)+" --> "+ str(error)))
            return dag_file_validation,config_dag_validation
        else:
            return dag_filename,dag_directory_path,dag_file_validation,\
                config_dag_filename,config_dag_directory_path,config_dag_validation
                
    def upload_to_gcs(self,gcs_file_name,source_file_path):
        """This function get Composer DAG GCS folder path and \
            uploads file to GCS bucket and file path"""
        """
        :param gcs_file_name: destination file name
        :param source_file_path: local source file path to upload files
        :return str config_dag_validation: Config DAG File validation flag Success or Fail
        """
        try:
            logging.info("Getting Composer Google Cloud Storage DAGS bucket ")
            authHeaders = self.google_api_headers()
            reqSession = self.createRequestSession()
            environment_url = (
                'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
                '/environments/{}').format(self.project_id, self.location, self.gcp_composer_env_name)
            response = reqSession.get(environment_url, headers=authHeaders,timeout=30)
            environmentVars = response.json()
            dagGcsPrefix = environmentVars['config']['dagGcsPrefix']
            GcsBucket = dagGcsPrefix.split("/")[-2]
            client = storage.Client(project=self.project_id)
            bucket = client.get_bucket(GcsBucket)
            gcsFolderPath = str(source_file_path.split("/")[-1])+"/"+gcs_file_name
            dag_blob = bucket.blob(gcsFolderPath)
            dag_blob.upload_from_filename(os.path.join(source_file_path,gcs_file_name))
            logging.info(f"File {gcs_file_name} uploaded to {GcsBucket}/{gcsFolderPath}.")
        except Exception as error:
            logging.error(" Got error while uploading file to GCS DAG folder : {}".format(str(type(error).__name__)+" --> "+ str(error)))


    def deploy_dag(self):
        """This function gets the GCS path for composer dags, \
            validates the file path for dag and uploads it to GCS Composer DAGS folder """
        
        dag_filename,dag_directory_path,dag_file_validation,\
                config_dag_filename,config_dag_directory_path,config_dag_validation = self.validate_file_path()
        
        if dag_file_validation == "success" and config_dag_validation == "success":
            self.upload_to_gcs(dag_filename,dag_directory_path)
            self.upload_to_gcs(config_dag_filename,config_dag_directory_path)
        elif dag_file_validation == "success" and config_dag_validation == "pass":
            self.upload_to_gcs(dag_filename,dag_directory_path)
        else:
            logging.error(" Please check file validation as dag_file_validation or config_dag_validation failed ")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s :: %(levelname)-8s :: [%(filename)s:%(lineno)d] :: %(message)s')
    parser = argparse.ArgumentParser(description= " Reading and parsing input variables ")
    parser.add_argument("-gcp_project_id",required=True,type=str, help=""" Google Cloud Project ID """)
    parser.add_argument("-gcp_composer_env_name",required=True,type=str, help=""" Cloud composer environment name """)
    parser.add_argument("-composer_env_location",required=True,type=str, help=""" Cloud Composer environment location """)
    parser.add_argument("-dag_file",required=True,type=str, help=""" DAG file to upload to GCS """)
    parser.add_argument("-dynamic_dag_flag",required=True,type=str, help=""" Dynamic DAG file, If Tue then please mention value for dynamic_dag_config_file """)
    parser.add_argument("-dynamic_dag_config_file",required=False,type=str, help=""" Dynamic DAG config file to upload to GCS """)
    args = parser.parse_args()
    argsDict = vars(args)
    dags = validate_deploy_dag(argsDict)
    dags.deploy_dag()