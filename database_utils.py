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

    def read_db_creds(self,creds: str='db_creds.yaml'):
        with open(creds,'r') as f:
            self.creds = dict(yaml.safe_load(f))
        return self.creds
     
    def init_db_engine(self):
        creds = self.read_db_creds()
        RDS_HOST = creds['RDS_HOST']
        RDS_PASSWORD = creds['RDS_PASSWORD']
        RDS_USER = creds['RDS_USER']
        RDS_DATABASE = creds['RDS_DATABASE']
        RDS_PORT = creds['RDS_PORT']
        RDS_DBAPI = creds['RDS_DBAPI']
        RDS_DATABASE_TYPE = creds['RDS_DATABASE_TYPE']
        self.engine = create_engine(f"{RDS_DATABASE_TYPE}+{RDS_DBAPI}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}")
        return self.engine
    
    def list_db_tables(self):
        with self.engine.execution_options(isolated_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' """))
        for table in result.fetchall():
            print(table) 





if __name__ == '__main__':
    dbs = DatabaseConnector()
    dbs.read_db_creds()
    dbs.init_db_engine()
    dbs.list_db_tables()
