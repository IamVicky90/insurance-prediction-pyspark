import os
import shutil
import yaml
def create_directory(path: str, is_recreate: bool = False)->None:
    """Utility to create the dirctory

    Args:
        path (str): Give the full path with directory name
        is_recreate (bool, optional): If True then it will first delete and then ceate the directory . Defaults to False.
    """
    if is_recreate:
        shutil.rmtree(path)
    os.makedirs(path,exist_ok=True) # It will not through error if the folder already exists
def read_params(config_path: str)->dict:
    """Responsible for reading the yaml file

    Args:
        config_path (str): Path of the Yaml file

    Returns:
        dict: Return the details of the yaml file
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)