from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import yaml
import pandas as pd
from sqlalchemy import create_engine,text,inspect
import pandas as pd 

tabula_pdf_connector = DataExtractor()
tabula_pdf = tabula_pdf_connector.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
tabula_pdf_cleaning = DataCleaning(tabula_pdf)
tabula_pdf_cleaning.get_unique_data_types()



rest_api = DataExtractor()
rest_api.