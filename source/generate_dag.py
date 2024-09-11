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
# [START generate_dag]

from jinja2 import Environment, FileSystemLoader
import os
import json
import argparse
import yaml

config_file = ''

# Read configuration file from command line
# Please refer to the documentation (README.md) to see how to author a
# configuration (YAML) file that is used by the program to generate
# Airflow DAG python file.
def configure_arg_parser():

    description = '''This application creates Composer DAGs based on the config file
        config.json and template. Extract Args for Run.'''
        
    parser = argparse.ArgumentParser(description= description)

    parser.add_argument('--config_file',
                        required=True,
                        help='''Provide template configuration YAML file location
                        e.g. ./config.yaml''')
    
    parser.add_argument('--dag_template',                         
                        default="standard_dag",
                        help="Template to use for DAG generation")
    
    parser.add_argument('--dynamic_config',
                        required=True,
                        help="Is this a dynamically configurable DAG during run-time?")
    
    parser.add_argument('--composer_env_name', 
                        help="Provide the composer env name from where the config values will be picked when dynamic_config=false")
    
    options = parser.parse_args()

    if (options.dynamic_config =="false" or options.dynamic_config =="False") and options.composer_env_name is None:
        parser.error('Requiring composer_env_name if dynamic_config is false')

    return options

# Generate Airflow DAG python file by reading the config (YAML) file
# that is passed to the program. This section loads a .template file
# located in the ./templates folder in the source and the template folder
# parses and dynamically generate a python file using Jinja2 template
# programming language. Please refer to Jinja documentation for Jinja 
# template authoring guidelines.
def generate_dag_file(args):

    config_file = args.config_file
    dag_template = args.dag_template
    dynamic_config = args.dynamic_config
    composer_env_name = args.composer_env_name
   
    with open(config_file,'r') as f:
        config_data = yaml.safe_load(f)
        config_file_name = os.path.basename(config_file)
        config_data["config_file_name"] = config_file_name
        config_path = os.path.abspath(config_file)
        file_dir = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(file_dir,"templates")
        dag_id = config_data['dag_id']

        print("Config file: {}".format(config_path))
        print("Generating DAG for: {}".format(dag_template))

        # Uses template renderer to load and render the Jinja template
        # The template file is selected from config_data['dag_template']
        # variable from the config file that is input to the program.
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(dag_template+".template")
        framework_config_values = {'dynamic_config': dynamic_config, 'composer_env_name': composer_env_name}

        dag_path = os.path.abspath(os.path.join(os.path.dirname(config_path), '..', "dags"))
        if not os.path.exists(dag_path):
            os.makedirs(dag_path)
            
        generate_file_name = os.path.join(dag_path, dag_id + '.py')
        with open(generate_file_name, 'w') as fh:
            fh.write(template.render(config_data=config_data, framework_config_values=framework_config_values))

        print("Finished generating file: {}".format(generate_file_name))
        # print("Number of tasks generated: {}".format(str(len(config_data['tasks']))))

if __name__ == '__main__':
    args = configure_arg_parser()

    generate_dag_file(args)

 # [END generate_dag]