import utility
from mongo_db import mongo_db_atlas
import pymongo
import os
from insurance_exception.insurance_exception import InsuranceException
import sys
class Mongo_operations:
    def __init__(self,username : str =None,password : str =None):
        """For handling mongo db databse operations

        Args:
            username (str, optional): username of the mongodb to get connection with mongodb atlas. Defaults to None.
            password (str, optional): password of the mongodb to get connection with mongodb atlas. Defaults to None.
        """
        try:
            if username is None or password is None:
            
                params=utility.read_params()
                credentials={
                    'username': params['mongodb_credentials_from_env_variables']['user_name'],
                    'password': params['mongodb_credentials_from_env_variables']['user_password']
                }
            self.__username=os.environ.get(credentials['username'])
            self.__password=os.environ.get(credentials['password'])
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, "__init__"), sys)
            
            raise error
    def get_connection_string(self)-> str:
        """To get the connection string to connect with mongodb

        Returns:
            str: return the connection string
        """
        return f"mongodb+srv://{self.__username}:{self.__password}@cluster0.hpbfo.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    def get_mongodb_client(self):
        """To get a MongoDB client
        Returns:
            [type]: Return a mongo client object
        """
        try:
            conn_string = self.get_connection_string()
            client=pymongo.MongoClient(conn_string)
            return client
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.get_mongodb_client.__name__), sys)
            
            raise error
    def close_mongodb_client(self,obj_name)->bool:
        """Close the MongoDB client connection

        Args:
            obj_name ([type]): object of mongodb client

        Returns:
            bool: Return True if close successfully
        """
        try:
            obj_name.close()
            return True
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.close_mongodb_client.__name__), sys)
            
            raise error
    def is_database_present(self,db_name: str)->bool:
        """Check if the database is present or not

        Args:
            db_name (str): name of the databse that you want to find.

        Returns:
            bool: True if the database is present otherwise False.
        """
        try:
            client=self.get_mongodb_client()
            if db_name in client.list_database_names():
                return True
            return False
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.is_database_present.__name__), sys)
            
            raise error
    def create_database(self,dbname: str):
        """Responsible to create a new database in mongodb

        Args:
            dbname (str): Name of the Database to create.

        Returns:
            object_type: Database Object
        """
        try:
            client=self.get_mongodb_client()
            database=client[dbname]
            return database
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.create_database.__name__), sys)
            
            raise error
    def create_collection(self, dbname : str, collection_name : str):
        """Responsible to create a new collection in mongodb

        Args:
            dbname (str): Name of the Database in which you want to create the collection.
            collection_name (str): Name of the collection to create in the following database.

        Returns:
            object_type: Collection Object
        """
        try:
            database=self.create_database(dbname)
            collection=database[collection_name]
            return collection
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.create_collection.__name__), sys)
            
            raise error

    def is_collection_present(self, collection_name: str, dbname: str) -> bool:
        """Check if collection  is present in the database or not

        Args:
            collection_name (str): Name of the collection to check its existence
            database (str): Name of the database.

        Returns:
            bool: True if collection is present in the database, False otherwise.
        """
        try:
            if self.is_database_present(dbname):
                client = self.get_mongodb_client()
                database=client[dbname]
                if collection_name in database.list_collection_names():
                    return True
                return False
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.is_collection_present.__name__), sys)
            
            raise error
    def is_record_present(self, dbname : str, collection_name : str,record_name: str)-> bool:
        """Check if the record exists in the collection of database or not.

        Args:
            dbname (str): Name of the Database
            collection_name (str): Name of the collection of database
            record_name (str): Give the record that you want to find.

        Returns:
            bool: True if Present, False otherwise
        """
        try:
            collection=self.create_collection(dbname,collection_name)
            if collection.find(record_name)>0:
                return True
            else:
                return False
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.is_record_present.__name__), sys)
            
            raise error
    def insert_one_record(self,record:dict,dbname:str,collection_name: str):
        """Responsible to insert one record at a time

        Args:
            record (dict): Record that will be inserted.
            dbname (str): Database name.
            collection_name (str): Name of the collection

        """
        try:
            collection=self.create_collection(dbname,collection_name)
            collection.insert_one(record)
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.insert_one_record.__name__), sys)
            
            raise error

    def insert_many_records(self, records: dict, dbname: str, collection_name: str):
        """Responsible to insert one record at a time

        Args:
            records (dict): Records that will be inserted.
            dbname (str): Database name.
            collection_name (str): Name of the collection

        """
        try:
            collection = self.create_collection(dbname, collection_name)
            collection.insert_many(records)
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.insert_many_records.__name__), sys)
            raise error

    def get_data_from_mongo_db(self,dbname, collection_name):
        """To get the data from Mongo_DB

        Args:
            dbname (str): Name of the database
            collection_name (str): Name of the collection that present in database
        """
        try:
            if self.is_database_present(dbname):
                if self.is_collection_present(collection_name,dbname):
                    client=self.get_mongodb_client()
                    collection=client[dbname][collection_name]
                    import pandas as pd
                    df=pd.DataFrame(collection.find({}))
                    return df
        except Exception:
            error = InsuranceException("Error in module {0} class {1} method {2}".format(
                Mongo_operations.__module__.__str__, Mongo_operations.__class__.__name__, self.get_data_from_mongo_db.__name__), sys)
            raise error

