import yaml
import pandas as pd
from sqlalchemy import create_engine,text,inspect
class DatabaseConnector:
    '''
   
which you will use to connect with and upload data to the database.

Parameters 
------------



Attribute
-----------

'''
    def __init__(self) -> None:
        pass

    def read_db_creds(self,yaml_path='db_creds.yaml'):
        try:
            with open(yaml_path,'r') as f:
                self.creds = dict(yaml.safe_load(f))
            return self.creds
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Error: Database credentials file '{self.creds}' not found.") from e
        except yaml.YAMLError as e:
            raise ValueError(f"Error: Invalid YAML format in credentials file '{self.creds}'.") from e 
        
    def init_db_engine(self):
        creds = self.creds 
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        RDS_DBAPI = creds['RDS_DBAPI']
        RDS_DATABASE_TYPE = creds['RDS_DATABASE_TYPE']
        self.engine = create_engine(f"{RDS_DATABASE_TYPE}+{RDS_DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}") # could add pool size and pool cycle to reduce the number of times you connect to the db 
        return self.engine
    
    def list_db_tables(self):
        with self.engine.execution_options(isolated_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' """))
        tables  = []
        for table in result.fetchall():
            tables.append(table) 
        return tables

    def upload_to_db(self,data_frame,table_name):
        '''
        Might need to initalise the init_db_engine method to get engine instead maybe pass it as parameter maybe
        '''
        creds_path = 'db_local_creds.yaml'
        upload_creds = self.read_db_creds(yaml_path=creds_path)
        upload_engine = self.init_db_engine()

        try:
            with upload_engine.execution_options(isolated_level='AUTOCOMMIT').connect () as conn:
                data_frame.to_sql(name=table_name,con =conn, if_exists= 'replace', index = False)
        except Exception as e:
            raise Exception(f"Error: uploading the table {table_name}") from e









# if __name__ == '__main__':
#     dbs = DatabaseConnector()
#     dbs.read_db_creds()
#     dbs.init_db_engine()
#     dbs.list_db_tables()
