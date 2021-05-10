from cassandra.cluster import Cluster
import sys
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the udacity
    - Returns the cluster and session to udacity keyspace
    """

    # This should make a connection to a Cassandra instance of your local machine 
    # (127.0.0.1)
    
    try :
        cluster = Cluster()

        # To establish connection and begin executing queries, need a session
        session = cluster.connect()
    except Exception as e :
        print(e)
        sys.exit("Exiting program : Issue in getting connection session to a cluster")

    # Create a Keyspace 
    try :
        session.execute(""" CREATE KEYSPACE IF NOT EXISTS udacity
                        WITH REPLICATION = {
                        'class': 'SimpleStrategy', 'replication_factor': 1 } """)
    except Exception as e:
        print(e)
        sys.exit("Exiting program : Issue in creating KeySapce ")

    # Set KEYSPACE to the cluster session specified above
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)
        sys.exit("Exiting program : Issue in setting Keyspace")
        

    return session, cluster

def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: issue dropping table")
            print(e)


def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: issue creating table")
            print(e)
            

def main():
    """
    - Creates if not exists, the udacity keyspace . 
    - Establishes connection with the udacity and gets connection session to execute query.  
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, shutdown the session and cluster. 
    """
    session, cluster = create_database()
    
    drop_tables(session)
    create_tables(session)

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()