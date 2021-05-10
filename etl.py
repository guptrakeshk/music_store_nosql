# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import csv
import numpy as np
from create_tables import create_database
from sql_queries import *



def generate_insert_columns (line, column_dict):
    """ This function genrates a column list for insertion into table with proper data type
    """
    insert_columns = []
    for key, value in column_dict.items() :
        if value == '' :
            insert_columns.append(line[key])
        elif value == 'float' :
            insert_columns.append(float(line[key]))
        elif value == 'int' :
            insert_columns.append(int(line[key]))
    return insert_columns
    
                                                    
                                                    
                                                    

def process_data_load(datafile, insert_query, column_dict, session):
    """ This function gets CSV data file, database insertion query and data column list and
        process CSV data to execute SQL query to load into Cassanda tables.
    """
    
    # Read the event data log file to fetch data and load into given database table 

    with open(datafile, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## Assign which column element should be assigned for each column in the INSERT statement.
            insert_column_list = generate_insert_columns(line, column_dict)
            
            # Convert column list into a tuple for values to be inserted into table
            session.execute(insert_query, tuple(insert_column_list))


def process_data(subfolder, outdatafile):
    """ This function identify the events log data filepath and creates a list of files to process.
    - It ensures to read only CSV data files from the filepath.
    - Reads CSV data one line at a time from each file at a time and append into a combined list.
    - Generates a new single processed CSV file with smaller event data subset.
    """
    # A list to store seleted event data from the CSV files.
    full_data_rows_list = [] 
    
    # checking your current working directory
    #print("Current Directory : ", os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/' + subfolder

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*.csv'))
        #print(file_path_list)

    
    # for every filepath in the file path list 
    for f in file_path_list:
        
        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

            # extracting each data row one by one and append it        
            for line in csvreader:
                full_data_rows_list.append(line) 

    # uncomment the code below if you would like to get total number of rows 
    #print('{} number of rows processed.'.format(len(full_data_rows_list)) )
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    #print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_new.csv that will be used \
    # to insert data into the Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(outdatafile, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


    '''
    
    # uncomment the code below if you would like to explore following:
    #
    # An alternative efficient way of reading multiple CSV files from a location and compiling
    # into a single CSV using Panda Library.
    # Note: Below code generates a single CSV file with full event data  

    
    column_labels = ['artist', 'firstName', 'gender', 'itemInSession','lastName', 'length', \
                    'level', 'location','sessionId', 'song', 'userId']   
    concat_df = pd.concat([ pd.DataFrame(pd.read_csv(f, encoding='utf8'), columns=column_labels) for f in file_path_list], ignore_index=True)

    # Write combined CSV data into a CSV file
    concat_df.to_csv(outdatafile, index=False)            
    '''        
            


def main():
    
    """ The main function to execute all steps in the completing the logical steps.
        - Create Cassandra keysapce and rerutn connection session along with cluster
        - Generate a single CSV data file from the event log CSV files after processing 
        - Process CSV data to generate data row that needs insertion into DB tables.
    """
        
    # Create database and get connection session to execute DB queries
    session, cluster = create_database()
    
    # Process event log data and generate a single CSV data file to load database tables
    events_out_file = 'event_datafile_new.csv'
    process_data('event_data',events_out_file)
    
    
    # music library data insertion
    # Create column data dictionary of a data row using index and value data type 
    # that needs insertion into table using insert query 
    column_dict = {0:'', 9:'', 5:'float', 8:'int', 3:'int'}
    process_data_load(events_out_file, music_library_insert, column_dict, session)
                          
    # user_music library data insertion
    # Create column data dictionary of a data row using index and value data type 
    # that needs insertion into table using insert query 
    column_dict = {0:'', 10:'int', 8:'int', 3:'int', 9:'', 1:'', 4:''}
    process_data_load(events_out_file, user_music_library_insert, column_dict, session)
                          
    # user_music library data insertion
    # Create column data dictionary of a data row using index and value data type 
    # that needs insertion into table using insert query 
    column_dict = {9:'', 1:'', 4:'',  0:'', 10:'int'}
    process_data_load(events_out_file, user_music_history_insert, column_dict, session)

    session.shutdown()
    cluster.shutdown()
    

if __name__ == "__main__":
    main()