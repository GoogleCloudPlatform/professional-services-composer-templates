import tokenize
import ast

def extract_keys_from_py_file(filename):
    """
    Extracts variable names (keys) assigned in a Python file,
    handling various assignment patterns.

    Args:
      filename: The path to the .py file.

    Returns:
      A list of variable names (keys).
    """
    keys = []
    try:
        with open(filename, "r") as f:
            tokens = tokenize.generate_tokens(f.readline)
            prev_token = None
            for token_type, token_string, _, _, _ in tokens:
                # Check for assignment (=)
                if token_type == tokenize.OP and token_string == "=":
                    # Go back to get the previous token (should be the variable name)
                    if prev_token and prev_token.type == tokenize.NAME:
                        # Add a check to skip if the variable name starts with "__" (dunder methods)
                        if not prev_token.string.startswith("__"):
                            keys.append(prev_token.string)
                prev_token = tokenize.TokenInfo(token_type, token_string, _, _, _)  # Store current token as previous

    except FileNotFoundError:
        print(f"Error: File not found - {filename}")
        # Handle file not found error appropriately (e.g., raise an exception, log, etc.)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # Handle other potential exceptions (e.g., log, re-raise, etc.)

    return keys


def process_condition(task, condition_type):
    """
    Reads 'precondition' or 'postcondition' from a task dictionary, 
    indents it (4 spaces), and adds extra indentation where necessary.
    Performs 'last_line_check' only for 'precondition'.
    """
    
    condition = task.get(condition_type)
    if condition:
        lines = condition.splitlines()
        
        updated_lines = []
        for line in lines:
            if line.startswith(" "):
                updated_lines.append("  " + line)
            else:
                updated_lines.append(line)
        updated_condition = "\n".join(updated_lines)
        last_line_indent = len(lines[-1]) - len(lines[-1].lstrip())

        if condition_type == 'pre_condition':
            last_line = lines[-1].strip()
            last_line_check = last_line.startswith("for") or last_line.startswith("if")
            return {
                'last_line_check': last_line_check,
                f'updated_{condition_type}': updated_condition, 
                'last_line_indent': last_line_indent 
            }
        elif condition_type == 'post_condition':
            return {
                f'updated_{condition_type}': updated_condition, 
                'last_line_indent': last_line_indent 
            }
    
    return None


def has_valid_default_args(filepath):
    """
    Checks if 'default_args' is defined and not empty in a Python file.

    Args:
        filepath: Path to the Python file.

    Returns:
        True if 'default_args' is defined and not empty, False otherwise.
    """
    with open(filepath, 'r') as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == 'default_args':
                    if isinstance(node.value, ast.Dict) and node.value.keys:
                        return True
                    else:
                        return False

    return False