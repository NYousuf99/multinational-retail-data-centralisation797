from database_utils import DatabaseConnector
import yaml
import pandas as pd
from sqlalchemy import create_engine,text,inspect
import tabula 
import requests
import time
import boto3

class DataExtractor:
    '''
    This class will work as a utility class, in it you will be creating methods that help extract data from different data sources.
    The methods contained will be fit to extract data from a particular data source, these sources will include CSV files, an API and an S3 bucket.

    Parameters 
    ------------



    Attribute
    -----------

    '''
    def __init__(self):
        pass

    def df_rds(self,tables):
        '''Tables is the return of the method list_db_tables in database utils file 
        '''
        self.df_tables = pd.DataFrame(tables)
        return self.df_tables
    
    def read_rds_table(self,engine,tables_name):
        try:
            with engine.execution_options(isolated_level='AUTOCOMMIT').connect() as conn:
                result = conn.execute(text(f"""SELECT * FROM {tables_name}"""))
            self.df = pd.DataFrame(result)
            return self.df
        except AttributeError as a:
            raise AttributeError(f"This Method requires an Engine attribute input from  init_db_engine() Method Found in the DatabaseConnector Class") from a

    
    def retrieve_pdf_data(self,pdf_url:str = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'):
        dfs = tabula.read_pdf(pdf_url,encoding='latin-1', pages='all')
        result = pd.concat(dfs,axis=0, ignore_index=True)
        return result

    def list_number_of_stores(self,url: str = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header:dict = {'x-api-key':"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
        response = requests.get(url,headers = header)
        if response.status_code == 200:
            data = response.json()

            return [data['number_stores']]
        else:
            print(f"Error retrieving number of stores: {response.status_code}")

    def retreive_stores_data(self,url:str="https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details",header:dict={'x-api-key':"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"},store_number:int=451):
        df = pd.DataFrame()
        for i in range(store_number):
            url_path = f"{url}/{i}"
            response = requests.get(url_path,headers = header)
            if response.status_code == 200:
                data = response.json()
                json2df= pd.json_normalize(data)
                df = pd.concat([df,json2df],axis=0)
            else:
                print(f"Error retrieving number of stores: {response.status_code}")
        return df

    def extract_from_s3(self,s3_url="s3://data-handling-public/products.csv"):
        bucket_name,object_key = s3_url.split('/')[-2:]
        s3_clinet = boto3.client('s3')
        try:
            response = s3_clinet.get_object(Bucket=bucket_name,Key=object_key)
            data_stream = response['Body']
            df = pd.read_csv(data_stream)
            return df 
        except Exception as e:
            message = f"Error extracting data from S3: {e}"
            raise Exception(message)

# if __name__ == '__main__':
#     db_connector = DatabaseConnector()
#     db_connector.read_db_creds()
#     engine = db_connector.init_db_engine()
#     tables = db_connector.list_db_tables()
#     db_extractor = DataExtractor()
#     db_extractor.df_rds(tables)
#     db_extractor.read_rds_table(engine)

