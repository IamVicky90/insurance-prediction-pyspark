import utility
import os
from spark_manager.spark_manager import Spark_Manager
class Data_Transformation:
    def __init__(self, logger, config):
        """Responsible for Data TRansformation.

        Args:
            logger (logger_obj): Object of the logger.
            config (str): path of the configuration file
        """
        self.__logger = logger
        self.__config = utility.read_params(config)
        # self.__logger.log("Data_Transformation started....")
        self.ss = Spark_Manager().get_SparkSession()
    def unite_data(self):
        """Responsible to combine all the batch training data into one sqllite db
        """
        path = self.__config["artifacts"]["Data_Directories"]\
                                                ["training"]\
                                        ["Good_Data_Directory"]
        files=os.listdir(path)
        # total_df=self.ss.createDataFrame(
        # data=[('', '', '', '', '', '', '')], schema=['age', 'sex', 'bmi', 'children', 'smoker', 'region', 'expenses'])
        i=0
        j=0
        for file in files:
            if file.endswith(".csv"):
                    if i==0:
                        df=self.ss.read.csv(os.path.join(path, file),header=True)
                        i=i+1
                        final_df = df
                        if j==1:
                            total_df = total_df.union(df)
                            df=total_df
                            final_df = total_df
                    else:
                        j=1
                        total_df = self.ss.read.csv(
                            os.path.join(path, file), header=True)
                        df = df.union(total_df)
                        total_df = df
                        i=i-1
                        final_df=df
        print(final_df.count())
        
if __name__ == "__main__":
    dt=Data_Transformation("logger_obj", os.path.join("config", "params.yaml"))
    dt.unite_data()

