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
import re

config_file = ''

def import_python_functions(yaml_config: dict) -> dict:
    """
    Generate a dictionary for importing Python functions into a DAG.

    This function determines how to import Python functions based on the
    provided YAML configuration. It can import functions from a specified
    file, from the YAML configuration, or from both. It also checks if
    'custom_defined_functions' exists and is not empty. It preserves
    indentation from the YAML "code" blocks.

    Args:
        yaml_config (dict): YAML configuration for the task.

    Returns:
        dict: A dictionary containing:
            - add_functions (bool): Flag indicating whether to add functions.
            - functions (list): A list of function code strings with preserved indentation.
    """
    functions = []
    try:
        custom_functions = yaml_config['custom_python_functions']
        if not isinstance(custom_functions, dict):
            raise TypeError("'custom_python_functions' should be a dictionary.")

        if custom_functions.get('import_functions_from_file', False) and custom_functions.get('functions_file_path'):
            file_path = custom_functions['functions_file_path']
            print(f"Importing Python Functions: reading python functions from file {file_path}")
            with open(file_path, 'r') as file:
                functions.append(file.read())

        defined_functions = custom_functions.get('custom_defined_functions')
        if defined_functions:
            for i, func_data in enumerate(defined_functions.values()):
                if 'code' in func_data and func_data['code'].strip():
                    # Split the code into lines and preserve indentation
                    code_lines = func_data['code'].splitlines()
                    functions.append('\n'.join(code_lines))  # Join the lines back
                    # functions.append(func_data['code'])

    except (KeyError, TypeError) as e:
        print(f"Importing Python Functions: Error processing YAML: {e}")

    if functions:
        return {
            "add_functions": True,
            "functions": functions
        }
    else:
        print("Importing Python Functions: Skipping function import.")
        return {"add_functions": False}


def validate_create_task_dependency(yaml_config: dict) -> dict:
    """Validate and create task dependency for the DAG.

    Args:
        yaml_config (dict): YAML configuration file for the task.

    Returns:
        dict: task_dependency_type (custom or default) to create task dependency.

    Raises:
        ValueError: If default_task_dependency is not boolean or 
                    if custom_task_dependency doesn't match config tasks.
    """
    try:
        default_dependency = yaml_config['task_dependency']['default_task_dependency']
        if default_dependency not in (True, False):
            raise ValueError("Invalid default_task_dependency value. "
                             "Acceptable values are True or False")
    except KeyError:
        raise ValueError("Missing 'task_dependency' or 'default_task_dependency' in yaml_config")

    task_list = ["start"]
    for task in yaml_config['tasks']:
        task_list.append(task['task_id'])
    task_list.sort()

    if default_dependency:
        return {"task_dependency_type": "default"}

    # Custom dependency
    try:
        custom_dependency = yaml_config['task_dependency']['custom_task_dependency']
    except KeyError:
        raise ValueError("Missing 'custom_task_dependency' in yaml_config")
    
    print("Task Validation: validating tasks for custom dependency")
    
    task_dependency = []
    custom_tasks = set()
    for dependency_chain in custom_dependency:
        # Remove the quotes from the dependency chain
        dependency_chain = dependency_chain.strip('"')  
        task_dependency.append(dependency_chain)  

        # Extract tasks 
        task_names = re.findall(r'[\w_]+', dependency_chain)  
        custom_tasks.update(task_names)


    # task_dependency = "\n".join(task_dependency)
    distinct_custom_tasks = sorted(list(custom_tasks))

    if distinct_custom_tasks != task_list:
        print(f"List of config tasks: {task_list}")
        print(f"List of custom tasks: {distinct_custom_tasks}")
        raise ValueError("Validation error: Tasks in custom_task_dependency "
                         "don't match config tasks")

    print("Task Validation: task validation successful")
    return {"task_dependency_type": "custom", "task_dependency": task_dependency}

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
    
    options = parser.parse_args()
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
   
    with open(config_file,'r') as f:
        config_data = yaml.safe_load(f)
        config_file_name = os.path.basename(config_file)
        config_data["config_file_name"] = config_file_name
        config_path = os.path.abspath(config_file)
        file_dir = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(file_dir,"templates")
        dag_id = config_data['dag_id']
        # Reading python function from .txt file or from YAML config as per configuration 
        python_functions = import_python_functions(yaml_config=config_data)

        # Importing task_dependency from YAML config as per configuration 
        task_dependency = validate_create_task_dependency(yaml_config=config_data)
        
        # Importing variables from variables.YAML or from YAML config as per configuration 
        var_configs = config_data["task_variables"]

        print("Config file: {}".format(config_path))
        print("Generating DAG for: {}".format(dag_template))

        # Uses template renderer to load and render the Jinja template
        # The template file is selected from config_data['dag_template']
        # variable from the config file that is input to the program.
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(dag_template+".template")
        framework_config_values = {'var_configs': var_configs}

        dag_path = os.path.abspath(os.path.join(os.path.dirname(config_path), '..', "dags"))
        if not os.path.exists(dag_path):
            os.makedirs(dag_path)
            
        generate_file_name = os.path.join(dag_path, dag_id + '.py')
        with open(generate_file_name, 'w') as fh:
            fh.write(template.render(config_data=config_data, framework_config_values=framework_config_values,\
                                      python_functions=python_functions, task_dependency=task_dependency))

        print("Finished generating file: {}".format(generate_file_name))
        print("Number of tasks generated: {}".format(str(len(config_data['tasks']))))

if __name__ == '__main__':
    args = configure_arg_parser()

    generate_dag_file(args)

 # [END generate_dag]