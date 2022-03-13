from mongo_db.mongo_db_atlas import Mongo_operations
from insurance_exception.insurance_exception import InsuranceException
from datetime import datetime
import sys
class Logger:
    def __init__(self,execution_id,executed_by,project_id,databasename,collection_name,is_log_enabled=True):
        try:
            self.execution_id = execution_id
            self.executed_by = executed_by
            self.project_id = project_id
            self.databasename = databasename
            self.collection_name = collection_name
            self.is_log_enabled = is_log_enabled
        except Exception:
                error = InsuranceException("Error in module {0} class {1} method {2}".format(
                    Logger.__module__.__str__, Logger.__class__.__name__, "__init__"), sys)
                self.log(error)
                raise error
            
    def log(self,message):
        try:
            if self.is_log_enabled:
                log_data={
                    "execution_id":self.execution_id,
                    "executed_by":self.executed_by,
                    "project_id":self.project_id,
                    "message":message,
                    'updated_date_and_time': datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
                }
                mongo_ops=Mongo_operations()
                mongo_ops.insert_one_record(log_data,self.databasename,self.collection_name)
            return 0
        except Exception as e:
            InsuranceException("Error in module {0} class {1} method {2}".format(
                Logger.__module__.__str__, Logger.__class__.__name__, self.log.__name__),e)
