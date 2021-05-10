## Data Engineering Project Using NoSQL Apache Cassandra 


### A Hands-On, Project-based instruction for Data Engineering ETL using NoSQL Cassandra

This is a collection of resources for data engineering ETL for a fictious a music company.

Email: gupt.rakeshk@gmail.com

This project walks through end-to-end data engineering steps that are needed in a typical project.
Model event log data to create a database and ETL pipeline in Cassandra NoSQL for a music streaming app. 
Define and model NoSQL tables and insert data into new tables.

#### Data Modeling and Design steps :

- Model your NoSQL Apache Cassandra database
- Design tables to answer the queries outlined in the project 
- Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
- Develop your CREATE statement for each of the tables to address each question
- Load the data with INSERT statement for each of the tables
- Include IF NOT EXISTS clauses in your CREATE statements to create tables only if the tables do not already exist. 
- Test by running the proper select statements with the correct WHERE clause


#### Building ETL Pipeline steps:

- Iterate through each event file in event_data to process and create a new CSV file in Python
- Generate Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in your data model
- Once data is cleansed, transformed and loaded, it is ready to asnwer specific queries for analytics.
- Test by running SELECT statements after running the queries on your database


#### How to execute programs :

Here are helpful steps in executing python programs in right sequence. You must execute **create_tables.py** first 
in order to create database tables which are needed for storing data.
1. execute `python create_tables.py` from CLI or other interface 
2. execute `python etl.py` from CLI or other interface 

#### Additional tips :
- One can use `test.ipynb` Jupyter notebook to validate data is successfully inserted into tables after ETL.


#### Datasets used :
This project uses one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

#### Project Artifacts and programs
- `create_tables.py` : 
- `sql_queries.py`
- `etl.py`



