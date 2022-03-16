from pyspark.sql import SparkSession
from insurance_exception.insurance_exception import InsuranceException as ManageSparkException
import sys
class Spark_Manager:
    spark_session=None
    def __init__(self,app_name="insurance_app"):
        """Responsible to manage the spark

        Args:
            app_name (str, optional): Name of the application. Defaults to "insurance_app".
        """
        self.app_name = app_name

    def get_SparkSession(self):
        """It will return the spark_session
        """
        try:
            if Spark_Manager.spark_session is None:
                Spark_Manager.spark_session =SparkSession.builder.master('local')\
                            .appName(self.app_name)\
                            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0")\
                            .config("spark.ui.port", "4041").getOrCreate()
            return Spark_Manager.spark_session
        except Exception:
            error = ManageSparkException("Error in module {0} class {1} method {2}".format(
                Spark_Manager.__module__, Spark_Manager.__class__.__name__, self.get_SparkSession.__name__), sys)
            raise error

if __name__ == "__main__":
    spark_manager=Spark_Manager()
    ss = spark_manager.get_SparkSession()
