import os
import json
import sys
import logging
import pandas as pd
import pyodbc
import re


def __find_odbc_driver(name_pattern='.* SQL Server'):
    """
    Return name of ODBC driver matching name_pattern regex.

    Keyword Arguments:
        name_pattern {str} -- regex for driver name (default: {'.* SQL Server'})

    Returns:
        {str} -- name of ODBC driver
    """
    driver_names = [x for x in pyodbc.drivers() if re.search(
        r"{}".format(name_pattern), x, flags=re.IGNORECASE)]

    if driver_names:
        return driver_names[0]
    else:
        return 'ODBC Driver 17 for SQL Server'


def azure_get_credentials(**kwargs):
    """
    Infer and/or update defaults for Azure SQL and Azure storage account
    defaults comes from credentials.json filepath set as environment variable 'AZURE_CREDENTIALS'

    Keyword Arguments:
        account_name {str} 'https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}'
        container_name {str} - storage container name
        account_key {str} - account_key for storage authentication

        server {str} -- azure sql server url e.g "myazuresql.database.windows.net"
        username {str} -- user id (default: None )
        password {str} -- password (default: None)
        database {str} -- database name (default: dev database)

    Returns:
    {dict} - credential dictionary

    """
    default_credpath = os.environ.get('AZURE_CREDENTIALS')

    if default_credpath:
        with open(default_credpath, 'r') as file:
            credentials = json.load(file)
    else:
        logging.warning(
            'default credentials path does not exit, no defaults set for authentication')
        credentials = {}

    if 'account_name' in kwargs:
        credentials['account_name'] = kwargs['account_name']

    if 'container_name' in kwargs:
        credentials['container_name'] = kwargs['container_name']

    if 'account_key' in kwargs:
        credentials['account_key'] = kwargs['account_key']

    if 'server' in kwargs:
        credentials['server'] = kwargs['server']

    if 'username' in kwargs:
        credentials['username'] = kwargs['username']

    if 'password' in kwargs:
        credentials['password'] = kwargs['password']

    if 'database' in kwargs:
        credentials['database'] = kwargs['database']

    return credentials


def azure_sql_con(**credentials):
    """creates connection to azure sql. use the connection with pandas.read_sql(sql_query,connection)

    Keyword Arguments:
        to override defaults set in environment variable 'AZURE_CREDENTIALS' below keyword arg should be provided
        server {str} -- azure sql server url e.g "myazuresql.database.windows.net" (default:)
        username {str} -- user id (default: admin )
        password {str} -- password (default: admin_password)
        database {str} -- database name (default: dev database)

    Returns:
        pyodbc connection to microsoft azure sql
    """
    credentials = azure_get_credentials(**credentials)  # dependency

    SERVER = credentials.get('server')
    database = credentials.get('database')
    username = credentials.get('username')
    password = credentials.get('password')
    driver_name = __find_odbc_driver(name_pattern='.* SQL Server')

    try:
        connection = pyodbc.connect('DRIVER={};SERVER=tcp:{};database={};UID={};PWD={}'.format(
            driver_name, SERVER, database, username, password))
        logging.info('connected to {} as {}'.format(database, username))
        return connection

    except Exception as e:
        logging.error(e)
        sys.exit('connection to azure sql failed')


def azure_to_df(sql_query, connection=None, **kwargs):
    """ Return result of sql query as pandas dataframe

    Arguments:
        sql_query {str} -- valid SELECT sql statement
        connection {pyodbc connection} -- uses azure_sql_con() function to create connection object
        kwargs {dict} -- key words arguments for pandas.read_sql function

    Returns:
        pandas.DataFrame
    """
    if not connection:
        try:
            connection = azure_sql_con()  # dependency
        except Exception as e:
            logging.error(
                e)
            logging.error(
                "not connected to azure, please use azure_sql_con() function to create connection object and pass it as a parameter to the function")
    return pd.read_sql(sql_query, connection, **kwargs)


def azure_sql_engine(**credentials):
    """creates sqlalchemy engine to write to azure sql. use the engine with df_to_azure_sql(df,table_name,engine)

    Keyword Arguments:
        to override defaults set in environment variable 'AZURE_CREDENTIALS' below keyword arg should be provided
        server {str} -- azure sql server url e.g "myazuresql.database.windows.net" (default: azure_sql_server)
        username {str} -- user id (default: admin )
        password {str} -- password (default: admin_password)
        database {str} -- database name (default: dev database)

    Returns:
        sqlalchemy engine to microsoft azure sql
    """
    import sqlalchemy
    from urllib.parse import quote_plus
    credentials = azure_get_credentials(**credentials)  # dependency

    server = credentials.get('server')
    database = credentials.get('database')
    username = credentials.get('username')
    password = credentials.get('password')
    driver_name = __find_odbc_driver(name_pattern='.* SQL Server')

    connection_string = quote_plus(f'DRIVER={driver_name}' +
                                   f';SERVER=tcp:{server};DATABASE={database};UID={username};PWD={password}')
    engine = sqlalchemy.create_engine(
        f"mssql+pyodbc:///?odbc_connect={connection_string}")
    logging.info('connected to {} as {}'.format(database, username))
    return engine


def df_to_azure_sql(df, table_name, engine=None, if_exists='fail', index=False, **kwargs):
    """ writes dataframe to Azure sql table
    table_name = 
    if_exists=


    Arguments:
        df {pandas.DataFrame} -- data to be written to table
        table_name {str} -- table name e.g 'dbo.fact_sample_superstore'

    Keyword Arguments:
        engine {sqlalchemy.engine} --  (default: {None})
        if_exists {str} -- write mode {'fail', 'replace', 'append'} (default: {'fail'})
        index {bool} -- if true, writes dataframe index as well (default: {False})
        keyword arguments for df.to_sql()

    Returns:
        str -- destination table_name
    """

    if not engine:
        logging.debug(
            'instantiating sql achemy engine with default credentials')
        engine = azure_sql_engine()  # dependency

    df.to_sql(table_name, engine, index=index, if_exists=if_exists, **kwargs)
    return table_name
