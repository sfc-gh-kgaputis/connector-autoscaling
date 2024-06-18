import os


def get_environment_variable(var_name, required=True, default=None):
    """
    Retrieves an environment variable.

    Args:
    var_name (str): The name of the environment variable.
    required (bool): Whether the environment variable is required.
    default: The default value to return if the variable is not found and not required.

    Returns:
    The value of the environment variable or the default value.

    Raises:
    EnvironmentError: If the variable is required but not found.
    """
    value = os.getenv(var_name)
    if value is None:
        if required:
            raise EnvironmentError(f"Required environment variable '{var_name}' not found.")
        return default
    return value
