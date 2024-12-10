import polars as pl
from typing import Dict, Any, Callable
import textwrap
import ast
import time


def remove_comments_and_docstrings(source: str) -> str:
    """
    Remove comments and docstrings from Python source code.

    Args:
        source: Python source code as string

    Returns:
        Cleaned Python source code
    """
    if not source.strip():
        return ""

    def remove_comments_from_line(line: str) -> str:
        """Remove comments while preserving string literals."""
        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(line):
            char = line[i]

            # Handle string boundaries
            if char in ('"', "'"):
                # Check for escaped quotes
                if i > 0 and line[i - 1] == '\\':
                    result.append(char)
                    i += 1
                    continue

                if not in_string:
                    # Check if it's the start of a string
                    in_string = True
                    string_char = char
                elif string_char == char:
                    # Check if it's the end of a string
                    in_string = False
                    string_char = None

            # Only process comment characters outside strings
            elif char == '#' and not in_string:
                break

            result.append(char)
            i += 1

        return ''.join(result).rstrip()

    # First pass: handle comments
    lines = [remove_comments_from_line(line) for line in source.splitlines()]
    source = '\n'.join(line for line in lines if line.strip())

    # Second pass: handle docstrings using AST
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source

    class DocstringRemover(ast.NodeTransformer):
        def visit_Module(self, node):
            # Remove module-level docstrings
            while (node.body and isinstance(node.body[0], ast.Expr)
                   and isinstance(node.body[0].value, ast.Constant)
                   and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_FunctionDef(self, node):
            # Remove function docstrings
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_ClassDef(self, node):
            # Remove class docstrings
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node):
            # Remove async function docstrings
            if (node.body and isinstance(node.body[0], ast.Expr)
                    and isinstance(node.body[0].value, ast.Constant)
                    and isinstance(node.body[0].value.value, str)):
                node.body.pop(0)
            return self.generic_visit(node)

        def visit_Expr(self, node):
            # Remove standalone string literals
            if isinstance(node.value, (ast.Str, ast.Constant)) and isinstance(getattr(node.value, 'value', None), str):
                return None
            return self.generic_visit(node)

    try:
        tree = DocstringRemover().visit(tree)
        ast.fix_missing_locations(tree)
        result = ast.unparse(tree)
        # Remove empty lines
        return '\n'.join(line for line in result.splitlines() if line.strip())
    except Exception:
        return source


class PolarsCodeParser:
    """
    Securely executes Polars code with restricted access to Python functionality.
    """

    def __init__(self):
        self.safe_globals = {
            # Polars functionality
            'pl': pl,
            'col': pl.col,
            'lit': pl.lit,
            'expr': pl.expr,
            # Basic Python built-ins
            'print': print,
            'len': len,
            'range': range,
            'enumerate': enumerate,
            'zip': zip,
            'list': list,
            'dict': dict,
            'set': set,
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'True': True,
            'False': False,
            'None': None,
            'time': time
        }

    @staticmethod
    def _validate_code(code: str) -> None:
        """
        Validate code for security concerns before execution.
        """
        try:
            tree = ast.parse(code)
            for node in ast.walk(tree):
                # Block imports
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    raise ValueError("Import statements are not allowed")

                # Block exec/eval
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        if node.func.id in {'exec', 'eval', 'compile', '__import__'}:
                            raise ValueError(f"Function '{node.func.id}' is not allowed")

                # Block access to system attributes
                if isinstance(node, ast.Attribute):
                    if node.attr.startswith('__'):
                        raise ValueError(f"Access to '{node.attr}' is not allowed")

        except SyntaxError as e:
            raise ValueError(f"Invalid Python syntax: {str(e)}")

    @staticmethod
    def _wrap_in_function(code: str) -> str:
        """
        Wraps code in a function definition, handling various input formats.
        """
        # Dedent the code first to handle various indentation styles

        # If it's a single expression or statement, wrap it appropriately
        if '\n' not in code.strip():
            # Handle expression that might return a value
            if any(code.startswith(prefix) for prefix in ['pl.', 'col(', 'input_df', 'expr(']):
                return f"def _transform(input_df):\n    return {code}"
            # Handle assignment to df
            else:
                return f"def _transform(input_df):\n    {code}\n    return output_df"

        # For multi-line scripts
        return (f"def _transform(input_df):\n" + '\n'.join(f"    {line}" for line in code.split('\n')) +
                '\n    return output_df')

    def get_executable(self, code: str) -> Callable:
        """
        Securely get a function that can be executed on a DataFrame.

        Args:
            code (str): The code to execute

        Returns:
            pl.DataFrame: The transformed DataFrame
        """
        # Validate the code first
        code = remove_comments_and_docstrings(code)
        code = textwrap.dedent(code).strip()
        self._validate_code(code)

        # Wrap the code in a function
        wrapped_code = self._wrap_in_function(code)

        try:
            # Create namespace for execution
            local_namespace: Dict[str, Any] = {}

            exec(wrapped_code, self.safe_globals, local_namespace)

            transform_func = local_namespace['_transform']
            return transform_func
        except Exception as e:
            raise ValueError(f"Error executing code: {str(e)}")

    def validate_code(self, code: str):
        """
        Validate code for security concerns before execution
        """
        code = remove_comments_and_docstrings(code)
        code = textwrap.dedent(code).strip()
        self._validate_code(code)


polars_code_parser = PolarsCodeParser()
