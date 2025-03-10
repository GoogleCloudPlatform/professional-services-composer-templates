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
import argparse
import yaml
import re
from utils import extract_keys_from_py_file, process_condition, has_valid_default_args

config_file = ''

def raise_exception(message):
    raise ValueError(message)


def reformat_yaml(yaml_data):
    """Reformats YAML data with an indentation of 4 spaces."""
    return yaml.dump(yaml_data, indent=4)


def import_variables(yaml_config: dict) -> dict:
    """
    Generate a dictionary for importing variables into a DAG.

    This function determines how to import variables based on the
    provided YAML configuration. It can import variables from a specified file, or from the YAML configuration. It also checks if 'default_args' is defined and not empty in the variables file.

    Args:
        yaml_config (dict): YAML configuration for the task.

    Returns:
        dict: A dictionary containing:
            - add_variables (bool): Flag indicating whether to add variables.
            - variables (list): A list of variables with preserved indentation.
            - variable_keys (list): A list of unique variable keynames from the imported variables
            - valid_default_args (bool): True if 'default_args' is defined and not empty, False otherwise.
    """
    variables = []
    variable_keys = []
    valid_default_args = False  # Initialize the flag here
    try:
        task_variables = yaml_config['task_variables']
        if not isinstance(task_variables, dict):
            raise TypeError("'task_variables' should be a dictionary.")

        if task_variables.get('variables_file_path'):
            file_path = task_variables['variables_file_path']
            variable_keys = extract_keys_from_py_file(file_path)
            print(f"Importing variables: reading variables from file {file_path}")

            # Check for valid 'default_args' here
            valid_default_args = has_valid_default_args(file_path)

            with open(file_path, 'r') as file:
                file_content = file.readlines()  # Read all lines into a list
                variables = [line for line in file_content if '# type: ignore' not in line]

    except (KeyError, TypeError) as e:
        print(f"Importing Variables: Error processing variables file: {e}")

    if variables:
        return {
            "add_variables": True,
            "variables": variables,
            "variable_keys": variable_keys,
            "valid_default_args": valid_default_args
        }
    else:
        print("Importing variables: Skipping variable import.")
        return {"add_variables": False}


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


def get_unique_tasks(config_data):
    """
    Extracts unique task IDs from a dictionary containing task and task group definitions.

    Args:
      config_data: A dictionary containing task and task group definitions.

    Returns:
      A list of unique task IDs, including task group IDs.
    """
    all_task_ids = []
    
    # Extract task IDs from regular tasks
    for task in config_data.get('tasks', []):
        all_task_ids.append(task['task_id'])

    # Extract task IDs from task groups and their tasks
    for group in config_data.get('task_groups', []):
        all_task_ids.append(group['group_id'])  # Add the group_id as a task
        if 'tasks' in group:
            for task in group['tasks']:
                all_task_ids.append(task['task_id'])

    # Return unique task IDs
    return sorted(list(set(all_task_ids)))


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
        if not isinstance(default_dependency, bool):  # More concise type check
            raise ValueError("Invalid default_task_dependency value. Acceptable values are True or False")
    except KeyError as e:
        raise ValueError(f"Missing key in yaml_config: {e}") from e  # More informative error message

    task_list = get_unique_tasks(yaml_config)

    if default_dependency:
        return {"task_dependency_type": "default"}

    try:
        custom_dependency = yaml_config['task_dependency']['custom_task_dependency']
    except KeyError as e:
        raise ValueError(f"Missing key in yaml_config: {e}") from e

    print("Task Validation: validating tasks for custom dependency")

    task_dependency = []
    custom_tasks = set()
    for dependency_chain in custom_dependency:
        dependency_chain = dependency_chain.strip('"')
        task_dependency.append(dependency_chain)
        custom_tasks.update(re.findall(r'[\w_]+', dependency_chain))

    if sorted(custom_tasks) != task_list:
        print(f"List of config tasks: {task_list}")
        print(f"List of custom tasks: {sorted(custom_tasks)}")  # Sort for consistent comparison
        raise ValueError("Validation error: Tasks in custom_task_dependency don't match config tasks")

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
        # Register the tag with the YAML parser
        tmp_config_data = yaml.safe_load(f)
        config_data = yaml.safe_load(reformat_yaml(tmp_config_data))
        config_file_name = os.path.basename(config_file)
        config_data["config_file_name"] = config_file_name
        config_path = os.path.abspath(config_file)
        file_dir = os.path.dirname(os.path.abspath(__file__))
        template_dir = os.path.join(file_dir,"templates")
        dag_id = config_data['dag_id']

        # Reading variables from .py variable file
        dag_variables = import_variables(yaml_config=config_data)
        # Reading python function from .txt file or from YAML config as per configuration 
        python_functions = import_python_functions(yaml_config=config_data)

        # Importing task_dependency from YAML config as per configuration 
        task_dependency = validate_create_task_dependency(yaml_config=config_data)
        
        # Importing variables from variables.YAML or from YAML config as per configuration 
        var_configs = config_data.get("task_variables")

        print("Config file: {}".format(config_path))
        print("Generating DAG for: {}".format(dag_template))

        # Uses template renderer to load and render the Jinja template
        # The template file is selected from config_data['dag_template']
        # variable from the config file that is input to the program.
        env = Environment(
            loader=FileSystemLoader(template_dir),
            lstrip_blocks=True,
        )

        # Consolidate functions in env.globals
        env.globals.update({
            'process_condition': process_condition,
            'raise_exception': raise_exception,
        })

        template = env.get_template(dag_template+".template")
        framework_config_values = {'var_configs': var_configs}

        dag_path = os.path.abspath(os.path.join(os.path.dirname(config_path), '..', "dags"))
        if not os.path.exists(dag_path):
            os.makedirs(dag_path)
            
        generate_file_name = os.path.join(dag_path, dag_id + '.py')
        with open(generate_file_name, 'w') as fh:
            fh.write(
                template.render(
                    config_data=config_data, 
                    framework_config_values=framework_config_values,python_functions=python_functions, 
                    task_dependency=task_dependency,
                    dag_variables=dag_variables,
                )
            )

        print("Finished generating file: {}".format(generate_file_name))


if __name__ == '__main__':
    args = configure_arg_parser()

    generate_dag_file(args)

 # [END generate_dag]