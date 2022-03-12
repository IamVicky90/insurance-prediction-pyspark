from asyncore import read
import os
import shutil
import yaml
from app_logger.logger import Logger
import uuid
def create_directory(path: str, is_recreate: bool = False)->None:
    """Utility to create the dirctory

    Args:
        path (str): Give the full path with directory name
        is_recreate (bool, optional): If True then it will first delete and then ceate the directory . Defaults to False.
    """
    if is_recreate:
        shutil.rmtree(path)
    os.makedirs(path,exist_ok=True) # It will not through error if the folder already exists
def read_params(config_path: str ='config/params.yaml')->dict:
    """Responsible for reading the yaml file

    Args:
        config_path (str): Path of the Yaml file . Defaults to 'config/params.yaml'

    Returns:
        dict: Return the details of the yaml file
    """
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def get_log_object_for_training(collection_name: str, execution_id : str=None, executed_by: str=None, project_id :str=None, is_log_enabled : bool=True) -> Logger:
    """It will give the Log Object for training

    Args:
        collection_name (str): Name of the collection in which the log will be stored
        execution_id (str, optional): Execution id. Defaults to None.
        executed_by (str, optional): Executed by. Defaults to None.
        project_id (str, optional): Id of the project. Defaults to None.
        is_log_enabled (bool, optional): If it is set to True then only it will write the logs. Defaults to True.

    Returns:
        Logger: Logger Object
    """
    params=read_params()
    if execution_id==None:
        execution_id=uuid.uuid4().hex
    if executed_by==None:
        executed_by=params['base']['author']
    if project_id==None:
        project_id = params['base']['project_id']
    logger_obj = Logger(execution_id=execution_id, executed_by=executed_by, project_id=project_id,
                        databasename=params['database_logs']['training_logs']['database_name'], collection_name=collection_name, is_log_enabled=is_log_enabled)
    return logger_obj

