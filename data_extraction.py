from database_utils import DatabaseConnector
import pandas as pd 
from database_utils import *


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

       

    def read_rds_table(self,tables):
        df_tables = pd.DataFrame(tables)
        return print(df_tables)


if __name__ == "__main__":

    db_connector = DatabaseConnector()
    tables = db_connector.list_db_tables()

    db_extractor = DataExtractor()
    db_extractor.read_rds_table(tables)



