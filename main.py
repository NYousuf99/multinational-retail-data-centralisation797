from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import yaml
import pandas as pd
from sqlalchemy import create_engine,text,inspect
import pandas as pd 



# #aws_data  task3
# aws_data_conn = DatabaseConnector()
# aws_data_conn.read_db_creds()
# engine = aws_data_conn.init_db_engine()
# tables = aws_data_conn.list_db_tables()

# aws_data_extr = DataExtractor()
# df_tables = aws_data_extr.df_rds(tables=tables)
# df = aws_data_extr.read_rds_table(engine=engine,tables_name=df_tables['table_name'][2])



# aws_data_clean = DataCleaning(df=df)
# replace_list = ['first_name','last_name']
# to_date_list = ['date_of_birth','join_date']

# for _ in range(2):
#     date_formats = aws_data_clean.date_formats(date_column=to_date_list[_])
#     aws_data_clean.valid_date_time(date_column=to_date_list[_],formats=date_formats)
#     aws_data_clean.ws_schr(column_name=replace_list[_],pattern='[^a-zA-Z]',replacement='')
#     if _ ==1:
#         aws_data_clean.ws_schr(column_name='phone_number',pattern='[^0-9]')
#         aws_data_clean.ws_schr(column_name='address',pattern='[^a-zA-Z0-9]',replacement=',')
#         aws_data_clean.ws_schr(column_name='company',pattern='[^a-zA-]')
#         aws_data_clean.reorder_columns(new_order=['user_uuid','first_name','last_name','date_of_birth','company','email_address','address','country','country_code','phone_number','join_date'])

# aws_data_clean.df = aws_data_clean.df[aws_data_clean.df['user_uuid'].apply(lambda x: len(x) == 36)]
# aws_data_clean.df.drop_duplicates(inplace=True)
# aws_data_clean.df.fillna('',inplace=True)
# aws_data_clean.df.drop(columns='company',inplace=True)
# aws_data_conn.upload_to_db(data_frame=aws_data_clean.df,table_name='dim_users')



# #pdf_data task4

# tabula_pdf_conn = DatabaseConnector()
# tabula_pdf_extr = DataExtractor()
# tabula_pdf = tabula_pdf_extr.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# tabula_pdf_cleaning = DataCleaning(tabula_pdf)
# tabula_pdf_cleaning.get_unique_data_types()
# tabula_pdf_cleaning.valid_number(number_column='card_number')
# tabula_pdf_cleaning.ws_schr(column_name='card_provider',pattern='[^a-zA-Z0-9]',replacement=' ')
# tabula_pdf_cleaning.ws_schr(column_name='card_provider',pattern='[0-9]',replacement='')
# tabula_pdf_cleaning.ws_schr(column_name='card_provider',pattern='digit',replacement='')
# format = tabula_pdf_cleaning.date_formats(date_column='date_payment_confirmed')
# tabula_pdf_cleaning.valid_date_time(date_column='date_payment_confirmed',formats=format)

# tabula_pdf_cleaning.df['expiry_date'] = pd.to_datetime(tabula_pdf_cleaning.df['expiry_date'],format='%m/%y',errors='coerce')
# tabula_pdf_cleaning.df.drop_duplicates(inplace=True)
# tabula_pdf_cleaning.df.fillna('',inplace=True)
# tabula_pdf_conn.upload_to_db(data_frame=tabula_pdf_cleaning.df,table_name='dim_card_details')
# #API_data task5

# rest_api = DataExtractor()
# rest_api_df = rest_api.retreive_stores_data()
# rest_api_cleaning =DataCleaning(rest_api_df)
# rest_api_cleaning.called_clean_store_data()
# rest_api_cleaning.df = rest_api_cleaning.df[rest_api_cleaning.df['continent'].apply(lambda x: x  in ['America','Europe'])]
# rest_api_cleaning.df.drop_duplicates(inplace=True)
# rest_api_cleaning.df.fillna('',inplace=True)
# rest_api_con = DatabaseConnector()
# rest_api_con.upload_to_db(data_frame=rest_api_cleaning.df,table_name='dim_store_details')
# #s3_data task6
# s3_conn = DatabaseConnector()
# s3_extra = DataExtractor()
# s3_df = s3_extra.extract_from_s3()
# s3_clean = DataCleaning(s3_df)
# s3_clean.weight_converter()
# s3_clean.reorder_columns(new_order=['uuid','product_code','product_name','product_price','weight','category','EAN','date_added','removed'])
# s3_conn.upload_to_db(data_frame=s3_clean.df,table_name='dim_products')

# #order_table task7
# products_conn = DatabaseConnector()
# products_conn.read_db_creds()
# engine = products_conn.init_db_engine()
# tables = products_conn.list_db_tables()
# products_extr = DataExtractor()
# df_tables = products_extr.df_rds(tables=tables)
# df = products_extr.read_rds_table(engine=engine,tables_name=df_tables['table_name'][3])
# products_cleaning = DataCleaning(df)
# products_cleaning.reorder_columns(new_order=['date_uuid','user_uuid','card_number','store_code','product_code','product_quantity'])
# products_conn.upload_to_db(data_frame=products_cleaning.df,table_name='orders_table')

#s3_json task8
s3_json_conn = DatabaseConnector()
url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
s3_json_df = pd.read_json(url)
s3_json_conn.upload_to_db(data_frame=s3_json_df,table_name='dim_date_times')

