from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import yaml
import pandas as pd
from sqlalchemy import create_engine,text,inspect
import pandas as pd 
import re

class DataCleaning :
    """
    A class containing methods for cleaning data from various data sources.
    """

    def __init__(self,df):
        self.df = df

    def get_unique_data_types(self):
        unique_typesd = {}
        for  column_name in self.df:
            unique_types = set(type(item) for item in self.df[column_name])
            unique_types.discard(set)
            unique_typesd[column_name] =  list(unique_types)
        return unique_typesd


    def date_formats(self,date_column:str): 
        '''
        This method 
        '''
        self.df[date_column].replace('[^a-zA-Z0-9]','-',regex=True,inplace=True)
        date_strings = set(list(self.df[date_column].replace('[0-9]','1',regex=True)))
        ls = []
        ts =[]
        for n,date_string in enumerate(date_strings):
            date_format = re.findall(r"[a-zA-Z0-9]+",date_string)
            if len(date_format)==3:
                format_string = ''
                for i in date_format:
                    try:
                        int(i)
                        if len(i)==4:
                            format_string +='%Y-'

                        elif len(i)==2 and '%m-' in format_string or '%B-' in format_string:
                            format_string +='%d-'

                        elif len(i) ==2:
                            format_string +='%m-'
                    except ValueError:
                        format_string +='%B-'
                ls.append(format_string)
                ts.append(format_string)
        for n,i in enumerate(ts):
            if '-%m-%d-' in i:
                format = i.replace(f'-%m-%d-',f'-%d-%m-')
                ls.append(format)
            elif '%m-%Y-%d-' == i :
                format = f'%d-%Y-%m-'
                ls.append(format)
            elif '%d-%Y-%m-' == i:
                format = f'%m-%Y-%d-'
                ls.append(format)
        lsv2 = list(set(ls))
        date_formats_list = [i[:-1] for i in lsv2]
        return date_formats_list


    def valid_date_time(self,date_column,formats:list):
      
        for n,i in enumerate(self.df[date_column]):
            for format in formats:
                try:
                    self.df[date_column].iloc[n] = pd.to_datetime(self.df[date_column].iloc[n], format=format)
                    break 
                except ValueError:
                    pass  

        for n,i in enumerate(self.df[date_column]):
            if type(i) != pd._libs.tslibs.timestamps.Timestamp:
                self.df[date_column].iloc[n] = ""
        return

    def valid_numberv2(self,number_column):
        """
        Remove non-numeric characters from the specified number column.
        """
        int_type_number = self.df[number_column].replace('[^0-9]','', regex=True)
        str_type_number = self.df[number_column].str.replace('[^0-9]','', regex=True)
        merged_numbers = str_type_number.combine_first(int_type_number)
        numeric_merged_numbers = pd.to_numeric(merged_numbers, errors='coerce')
        self.df[number_column] = numeric_merged_numbers
        return self.df

    def valid_number(self,number_column,replace = '[^0-9]',replace_with = ''):
        """
        Remove non-numeric characters from the specified number column.
        """
        self.df[number_column] = self.df[number_column].astype('str')
        self.df[number_column]= self.df[number_column].replace(replace,replace_with,regex=True)
        
        return self.df


    def general_clean(self,column_name,pattern ='^[a-zA-z0-9]'):
        """
        Perform general data cleaning tasks.
        """
        self.df[column_name].replace(pattern,regex= True, inplace=True)
        self.df.drop_duplicates(inplace=True)
        self.df.fillna('',inplace=True)
        return self.df
            
        

    
    def ws_schr(self,column_name,leading_value ='',trailing_value='',pattern ='[^ a-zA-Z0-9]',replacement=''):
        """
        Removes leading/trailing space and special characters.
        """
        self.df[column_name]=self.df[column_name].str.lstrip(leading_value)
        self.df[column_name]=self.df[column_name].str.rstrip(trailing_value)
        # input_value = input(f"Would you like to remove unique characters in this column? Y/N:")
        # input_value = input_value.title()
        input_value = 'Y'
        if input_value == 'Y':
            self.df[column_name].replace(pattern,replacement, regex= True, inplace = True)
        return self.df

    def remove_rows(self,column_name,removal_list):
        self.df.dropna(subset=[column_name],inplace=True)
        return self.df
    
    def remove_random_chrs(self,column_name,replace='\n',replace_w=' ',pattern='[^a-zA-Z0-9]'):
        self.df[column_name].replace(replace,replace_w,regex=True,inplace=True)
        self.df[column_name].replace(pattern,replace_w,regex=True,inplace=True)
        return self.df
    
    def remove_columns(self,column_name):
        self.df.drop(column_name,axis=1,inplace=True)
        return self.df

    def reorder_columns(self,new_order=['store_code','staff_numbers','store_type', 'address', 'latitude', 'longitude', 'locality', 
              'opening_date',
             'country_code', 'continent']):
        self.df = self.df.reindex(columns=new_order)
        return self.df


    def called_clean_store_data(self,date_column='opening_date',formats = [],number_columns =['staff_numbers','longitude','latitude'],leading_values='ee',column_con='continent',drop_column='lat',replace_column='address'):
        format_dates = self.date_formats
        date_cleaner = self.valid_date_time
        number_cleaner = self.valid_number
        trail_chr_cleaner = self.ws_schr
        remove_random = self.remove_random_chrs
        remove_null_columns = self.remove_columns
        general_clean = self.general_clean
        reoder_columns = self.reorder_columns
        remove_random(column_name = replace_column)
        remove_null_columns(drop_column)
        formats = format_dates(date_column)
        date_cleaner(date_column,formats)
        for i in number_columns:
            number_cleaner(number_column = i)
        trail_chr_cleaner(column_name=column_con,leading_value=leading_values)
        reoder_columns()
        return  self.df

    def operation(self,value):
        try:
            if 'x' in value:
                new_value = value.split('x')
                new_i = float(new_value[0])*float(new_value[1])
                return new_i
        except:
            return value
    def weight_converter(self):
        units = ['kg','g','ml','oz']
        new_weights = []
        for i in self.df['weight']:
            if units[0] in str(i):
                new_i = i.replace('kg','').replace(' ','')
                new_i = self.operation(new_i)
                new_weights.append(f"{new_i}kg")
            elif units[1] in str(i):

                new_i = i.replace('g','').replace(' ','')
                new_i = self.operation(new_i)
                new_i = float(new_i)/1000
                new_weights.append(f"{new_i}kg")
            elif units[2] in str(i):
                new_i = i.replace('ml','').replace(' ','')
                new_i = self.operation(new_i)
                new_i = float(new_i)/1000
                new_weights.append(f"{new_i}kg")
            elif units[3] in str(i):
                new_i = i.replace('oz','').replace(' ','')
                new_i = self.operation(new_i)
                new_i = float(new_i)*0.0283
                new_weights.append(f"{new_i}kg")
            else:
                new_weights.append('')
        self.df['weight'] = new_weights
        return self.df




# if __name__ == '__main__':
#     db_connector = DatabaseConnector()
#     db_connector.read_db_creds()
#     engine = db_connector.init_db_engine()
#     tables = db_connector.list_db_tables()
#     db_extractor = DataExtractor()
#     db_extractor.self.df_rds(tables)
#     db_query = db_extractor.read_rds_table(engine)
#     cleaner = DataCleaning(db_query)
#     cleaner.valid_date_time('date_column')
#     cleaner.valid_number('number_column')
#     cleaner.general_clean('user_id_column')
#     cleaner.remove_blanks_nulls() 
