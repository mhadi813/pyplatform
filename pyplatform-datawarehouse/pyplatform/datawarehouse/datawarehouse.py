import os
import re
import pandas as pd
from google.cloud import bigquery
import logging
import datetime
import pytz
import json
import io


def bq_to_df(sql, client=None, **job_config):
    """Return bigquery query result as pandas.DataFrame.

    Arguments:
        sql {str} -- bigquery SELECT statement in standard SQL or stored procedure containing SELECT statement

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        job_config {dict} -- keyword arguemnt for bigquery.job.QueryJobConfig

    Returns:
        pandas.DataFrame -- query result as df

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    def get_date_columns(job):
        """Return list of DATE columns from bigquery.job object."""
        return [field.name for field in job.result().schema if field.field_type == 'DATE']

    def transform_date(date_columns):
        """Caste datetime.date to pd.datetime as pantab doesn't handle datetime.date objects."""
        for col in date_columns:
            df[col] = pd.to_datetime(df[col])

    job_config = bigquery.QueryJobConfig(**job_config)

    dry_run = bigquery.QueryJobConfig(
        dry_run=True, use_query_cache=False)
    job = client.query(sql, job_config=dry_run)

    if job.statement_type == 'SELECT':
        job_id = create_bq_job_id(
            'adhoc SELECT Statment request')  # dependency

        job = client.query(sql, job_id=job_id, job_config=job_config)
        # job.result()
        df = job.to_dataframe()
        # job_info = bq_get_job_info(job,client=client) #dependency
        date_columns = get_date_columns(job)

        if date_columns:
            transform_date(date_columns)

    elif job.statement_type == 'SCRIPT':
        job_id = create_bq_job_id(
            'adhoc script request')  # dependency
        job = client.query(sql, job_id=job_id, job_config=job_config)
        job.result()
        job_id = bq_get_job_info(job, client=client,
                                 output_option='LIST')  # dependency

        if len(job_id) == 1:
            df = client.get_job(job_id[0]).to_dataframe()
            # job_info = bq_get_job_info(job,client=client) #dependency
            date_columns = get_date_columns(job)

            if date_columns:
                transform_date(date_columns)

        elif len(job_id) > 1:
            df = client.get_job(job_id[-1]).to_dataframe()
            # job_info = bq_get_job_info(job,client=client) #dependency
            logging.warning(
                " multi select stored procedure returns data for the last SELECT statement only")
            date_columns = get_date_columns(job)

            if date_columns:
                transform_date(date_columns)

        else:
            logging.error(f"{sql} script did not return any data")
            df = None
    else:
        logging.error(
            f"{job.statement_type} are not suppoted. Please provide a SELECT statement or a stored procedure containing SELECT statement")
        return
        # job_info = bq_get_job_info(job,client=client) #dependency
        # logging.info(f"job execution detail: {job_info}")
    logging.info(f"{job.statement_type} statement returned {len(df)} rows")

    return df


def bq_to_df_with_json_objects(sql=None, job_id=None, client=None, output_option='DF', json_file_name=None):
    """Return nested and repeated fields as pandas.DataFrame, JSON string or json file either from sql SELECT statement or job_id.

    Keyword Arguments:
        sql {str} -- bigquery SELECT statement in standard SQL
        job_id {str} -- custom job_id for select statement. If result of completed job is needed, sql statement should be omitted to get result by job_id
        client {bigquery.Client} -- defaults to client instantiated with default credentials 
        output_option {str} -- {'DF','FILE','JSON','IO'} (default: {'DF'}) 
            DF => pandas.dataframe
            FILE => json file in current working directory
            JSON => json string
            IO => io.StringIO

        json_file_name {str} -- optional filename if FILE output is choosen (default: {Result_YYYYMMDD_HHMMSS_EST.json})

    Returns:
        pandas.DataFrame|JSON|filename

    Example:

    sql = f"select * from {table_id}"

    bq_to_df_with_json_objects(sql) # returns dataframe
    bq_to_df_with_json_objects(script_job_id,'JSON') # returns JSON object from Script statmente job_id
    bq_to_df_with_json_objects(sql, output_option='FILE', json_file_name='dowlonad_jsonfile.json'), dowloaded to file 
    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    def datetime_transformer(obj):
        """Caste datetime.date and datetime.datetime object to iso string."""
        if isinstance(obj, datetime.datetime) or isinstance(obj, datetime.date):
            return obj.isoformat()

    if job_id and not sql:
        query_job = client.get_job(job_id)
    else:
        query_job = client.query(sql, job_id=job_id)

    records = [dict(row) for row in query_job]
    if not json_file_name:
        ts_str = datetime.datetime.now(pytz.timezone(
            'America/New_York')).strftime('%Y%m%d_%H%M%S_EST')
        json_file_name = f'result_{ts_str}.json'

    if output_option == 'FILE':
        with open(json_file_name, mode='w') as file:
            json.dump(records, file, default=datetime_transformer)
        return json_file_name
    elif output_option == 'IO':
        in_mem_file = io.StringIO()
        json.dump(records, in_mem_file, default=datetime_transformer)
        in_mem_file.seek(0)
        return in_mem_file
    elif output_option == 'JSON':
        return json.dumps({"data": records}, default=datetime_transformer)
    else:
        return pd.DataFrame(records)


def bq_result_to_table(sql, destination_table_id, write_mode='WRITE_APPEND', client=None, **job_config):
    """Write query result to a permanent bigquery table.

    Arguments:
        sql {str} -- bigquery standard sql SELECT statements; stored procedure? #TODO
        destination_table_id {str} -- fully qualified table id of target write table e.g. project_id.dataset.new_tablename
        write_mode {str} -- optional argument. Valid values are 'WRITE_APPEND' , 'WRITE_TRUNCATE' OR 'WRITE_EMPTY'. defaults to WRITE_APPEND mode
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        job_config {dict} -- keyword arguemnt for bigquery.job.QueryJobConfig

    Returns:
        bigquery.job.QueryJob
    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(**job_config)
    job_config.write_disposition = write_mode
    job_config.destination = destination_table_id

    job_id = create_bq_job_id("{}_{}".format(
        write_mode, destination_table_id.split(".")[-1]))  # dependency

    job = client.query(
        sql,
        job_config=job_config, job_id=job_id)

    return job


def bq_to_excel(sql, filepath=None, sheet_name=None, index=False, mode='w', client=None, output_option='FILE'):
    """Downloads bigquery query result as excel file from sql statement, script or stored procedure.

    Arguments:
        sql {str} -- bigquery SELECT statement in standard SQL or spoc containing SELECT statement

    Keyword Arguments:

        filepath {str} -- custom filename for downloaded results (default: yyyymmdd_hhmmss_EST_result.xlsx)
        sheet_name {str} --  (default: {'Sheet1'})
        index {bool} -- if set to True, keeps dataframe index in the output file  (default: {False})
        mode {str} -- {'w', 'a'}, default 'w'
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        output_optinos {str} -- {'FILE','IO'}
            FILE write to file on disk
            'IO' returns io.BytesIO
    Returns:
        {str, io.BytesIO} -- filepath of downloaded file or IO
    """

    job_id = create_bq_job_id("adhoc_query_to_excel")  # dependency
    if filepath == None:
        filepath = job_id[:19]+'_result.xlsx'

    if output_option == 'IO':
        filepath = io.BytesIO()  # in_mem_file

    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job = client.query(sql)
    job.result()

    if job.statement_type == 'SCRIPT':
        select_job_ids = bq_get_job_info(
            job, output_option='LIST')  # dependency
        dfs = [client.get_job(job_id).to_dataframe()
               for job_id in select_job_ids]
    else:
        dfs = [job.to_dataframe()]

    dfs_to_excel(dfs, filepath, sheet_name=sheet_name, mode=mode)  # dependency
    if output_option == 'IO':
        filepath.seek(0)

    return filepath


def bq_to_csv(sql, filepath=None, header=True, client=None, output_option='FILE'):
    """Download bigquery query result as csv file or io.stringIO.

    Arguments:
        sql {str} -- bigquery SELECT statement in standard SQL

    Keyword Arguments:
        filepath {str} -- custom filename for downloaded results (default: yyyymmdd_hhmmss_EST_result.csv)
        header {bool} -- if set to True, keeps headers in the output file (default: {True})
        client {bigquery.Client} -- defaults to client instantiated with default credentials 
        output_option {str} -- {FILE','IO'} (default: {'FILE'}) 
            FILE => CSV file in current working directory
            IO => io.StringIO

    Returns:
        {str} -- filepath of downloaded file
    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job_id = create_bq_job_id("adhoc_query_to_csv")  # dependency
    if filepath == None:
        filepath = job_id[:19]+'_result.csv'
    job = client.query(sql)
    records = [dict(row) for row in job]
    df = pd.DataFrame(records)

    if output_option == 'IO':
        filepath = io.StringIO()

    df.to_csv(filepath, header=header, index=False,
              encoding='utf-8', date_format='iso')
    if output_option == 'IO':
        filepath.seek(0)

    return filepath


def bq_get_job_info(job, client=None, output_option=None):
    """Return bigquery job info.

        For query jobs: 
            returns job_id, statement_type, number_of_row_affected, destination_table_path and children job info
        For laod jobs: 
            return job_id, job_type, destination_table, write_mode, number of output rows and errors encountered

    Arguments:
        job {bigquery.job} -- query job or load job

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        output_option {str} -- {'LIST','DICT'} affects output info for SCRIPT statement query jobs. 
            'LIST' retruns filtered list of children job_id of SELECT type where result total_row > 1. 
            'DICT' returns details of children jobs of SELECT type (default: {None})

    Returns:
        dict | list -- job info is dict, while children job info is list

    Example:
    bq_get_job_info(client.query(sql_statement)) # returns job_info

    # for SCRIPT, returns detail of children jobs of SELECT statements type
    bq_get_job_info(script_job, output_option='DICT')

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    def extract_table_id(job_destination_path):
        return f"{job_destination_path.split('/')[2]}.{job_destination_path.split('/')[4]}.{job_destination_path.split('/')[6]}"

    if job.job_type == 'query':
        if job.statement_type == 'SCRIPT':
            child_jobs_iterable = client.list_jobs(parent_job=job)
            children = [{'job_id': job.job_id, 'statement_type': job.statement_type, 'destination': extract_table_id(job.destination.path),
                         'num_dml_affected_rows': job.num_dml_affected_rows} if job.statement_type != 'SELECT' else {'job_id': job.job_id, 'statement_type': job.statement_type,
                                                                                                                     'total_rows': job.result().total_rows, 'destination': extract_table_id(job.destination.path), 'schema': get_table_schema_from_bq(extract_table_id(job.destination.path), client=client, output_option='DICT')} for job in child_jobs_iterable]
            data = {'job_id': job.job_id, 'statement_type': job.statement_type,
                    'num-child_jobs': job.num_child_jobs, 'child_job': children}

            if output_option == 'LIST':
                children_job_id_SELECT = [job.get('job_id') for job in data.get(
                    'child_job') if job.get('statement_type') == 'SELECT' and job.get('total_rows') > 1]
                data = children_job_id_SELECT

            elif output_option == 'DICT':
                children_job_id_SELECT = [job.get('job_id') for job in data.get(
                    'child_job') if job.get('statement_type') == 'SELECT']
                data = [{'job_id': child, 'statement_type': 'SELECT', 'total_rows': client.get_job(child).result(
                ).total_rows, 'destination': extract_table_id(client.get_job(child).destination.path)} for child in children_job_id_SELECT]

        elif job.statement_type == 'SELECT':
            data = {'job_id': job.job_id, 'statement_type': job.statement_type,
                    'total_rows': job.result().total_rows, 'destination': extract_table_id(job.destination.path), 'schema': get_table_schema_from_bq(extract_table_id(job.destination.path), client=client, output_option='DICT')}
        else:
            data = {'job_id': job.job_id, 'statement_type': job.statement_type,
                    'destination': extract_table_id(job.destination.path), 'num_dml_affected_rows': job.num_dml_affected_rows}
    else:
        data = {'job_id': job.job_id, 'job_type': job.job_type,
                'destination': extract_table_id(job.destination.path), 'write_mode': job.write_disposition, 'output_rows': job.output_rows, 'errors': job.errors}
    return data


def df_to_bq(df, table_id, client=None, write_mode='WRITE_APPEND', schema=None, autodetect=True, job_id=None, **job_config):
    """Write DataFrame to bigquery table with custom schema.

    Arguments:
        df {pd.DataFrame} -- pandas dataframe as source
        table_id {str} -- fully qualified table_id as write destination e.g. project_id.dataset.table_name

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        write_mode {str} -- {'WRITE_APPEND' , 'WRITE_TRUNCATE' , 'WRITE_EMPTY'}. (default: {'WRITE_APPEND'})
        schema {list} -- list of bigquery.schema.SchemaField 
            Partial schema is acceptable. 
            Use get_table_schema_from_bq(table_id) or get_table_schema_from_df(df) to enforce existing table schema
        autodetect {bool} -- if True, bigquery infers datatype, (default: {'True'})
        job_id {str} -- optional argument, use function create_bq_job_id(description=None) to create custom job id
        job_config {dict} -- any other keyowrd argument for bigquery.job.LoadJobConfig

    Returns:
        biqquery.LoadJob

        """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(**job_config)
    job_config.autodetect = autodetect
    job_config.write_disposition = write_mode

    if schema:
        job_config.schema = schema

    load_job = client.load_table_from_dataframe(
        df, table_id, job_id=job_id, job_config=job_config)
    return load_job


def df_to_bq_with_json_objects(df, table_id, client=None, schema=None, json_str_column=None, write_mode='WRITE_APPEND', job_id=None, **job_config):
    """Write DataFrame to bigquery with nested and repeated fields or JSON objects.

    Arguments:
        df {pd.DataFrame} -- pandas dataframe to be updoad
        table_id {str} -- fully qualified bq table id as write destination e.g. project_id.dataset.new_tablename

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        write_mode {str} -- {'WRITE_APPEND' , 'WRITE_TRUNCATE' , 'WRITE_EMPTY'}. (default: {'WRITE_APPEND'})
        schema {list} -- list of bigquery.schema.SchemaField 
            Partial schema is acceptable. 
            Use get_table_schema_from_bq(table_id) or get_table_schema_from_df(df) to enforce existing table schema
        job_id {str} -- optional argument, use function create_bq_job_id(description=None) to create custom job id for logging
        job_config {dict} -- any other keyowrd argument for bigquery.job.LoadJobConfig

    Returns:
        bigquery.job.LoadJob

    Example:
    table_id = 'project_id.dataset.movie_review_json'
    job_config = {'max_bad_records': 5,'autodetect': False}
    schema = schema = get_table_schema_from_df(df)
    client = bigquery.Client().from_service_account_json(cred_filepath)
    job = df_to_bq_with_json_objects(df,table_id,schema=schema,client=client,**job_config)
    job.result()

    """
    from io import StringIO

    def get_datetime_columns(schema):
        """Return list TIMESTAMP columns from list of bigquery.schema.SchemaField
        """
        return [field.name for field in schema if field.field_type in ['TIMESTAMP']]

    def get_date_columns(schema):
        """Return list of DATE columns from list of bigquery.schema.SchemaField
        """
        return [field.name for field in schema if field.field_type in ['DATE']]

    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    if schema:
        datetime_columns = get_datetime_columns(schema)
        date_columns = get_date_columns(schema)
    else:
        datetime_columns = None
        date_columns = None

    if datetime_columns:
        #         logging.debug(f"{datetime_columns} will be casted to %Y-%m-%d %H:%M:%S.%f string")
        for column in datetime_columns:
            # df[column].apply(lambda x: x.isoformat())
            df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S.%f')

    if date_columns:
        #         logging.debug(f"{date_columns} will be casted to %Y-%m-%d string")
        for column in date_columns:
            # datetime.date object is problematic
            df[column] = pd.to_datetime(df[column])
            # df[column].apply(lambda x: x.isoformat())
            df[column] = df[column].dt.strftime('%Y-%m-%d')

    if json_str_column:
        if isinstance(json_str_column, list):
            for column in json_str_column:
                df[column] = df[column].apply(json.dumps)
        else:
            column = json_str_column
            df[column] = df[column].apply(json.dumps)

    # ,date_format='%Y-%m-%dT%H:%M:%S.%f'
    buffer = StringIO(df.to_json(orient="records", lines=True))

    job_config = bigquery.LoadJobConfig(**job_config)

    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = write_mode

    if schema:
        job_config.schema = schema

    logging.debug(f'Load job config: \n {job_config.to_api_repr()}')
    load_job = client.load_table_from_file(
        buffer, table_id, job_id=job_id, job_config=job_config)
    return load_job


def dfs_to_excel(dfs, file, sheet_name=None, index=False, mode='w'):
    """ writers one or more dataframes to excel sheet(s)

    Arguments:
        dfs {pandas dataframe or list of dataframes} -- each dataframe will be written on a separate sheet
        file {str or io.BytesIO} -- excel filepath with .xlsx or xls extension or io.BytesIO (in_memory_file)

    Keyword Arguments:
        sheet_name {str or list} -- custom sheet name for each dataframe, number of sheet name must match number of data frames 
        or else default sheet name will be used  (default: {Sheet 1, Sheet 2 ...})
        index {bool or list} -- if ture, writes dataframe index. For multi-sheet list of bool should be provided. By default index is ignored (default: {False})
        mode {str} -- {'w' = overwrite or creates new file, 'a' = append to existing file}, (default: {'w'}) #TODO validation

    Example:
    file_name= 'file_with_many_sheets.xlsx'
    dfs=[df1,df2,df3,df4,df5]
    names=['trash1','trash2','trash3','trash4','trash5']
    dfs_to_excel(dfs,file_name,names)
    """
    if isinstance(dfs, list):
        num_sheet = len(dfs)
        if isinstance(index, bool):
            index = [False for i in range(num_sheet)]
    else:
        num_sheet = 1
        dfs = [dfs]
        index = [index]

    if num_sheet == 1 and sheet_name and type(sheet_name) == str:
        sheet_name = [sheet_name]  # single custom sheet name
    elif num_sheet > 1 and type(sheet_name) == list and num_sheet == len(sheet_name):
        pass  # multi-sheet file with custom name; all good
    else:
        sheet_name = [f'Sheet {n+1}' for n in range(num_sheet)]

    with pd.ExcelWriter(path=file,  engine="openpyxl", date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD HH:MM:SS', mode=mode) as writer:
        for i in range(num_sheet):
            dfs[i].to_excel(writer, sheet_name=sheet_name[i], index=index[i])


def bq_load_gcs_csv(source_uri, destination_table_id, schema=None, client=None, job_id=None, **job_config):
    """Upload csv file from google cloud storage bucket to Bigquery table.

    Arguments:
        source_uri {str} -- google cloud storage source uri(s). use wildcard (*) to load multiple files e.g. 'gs://bucketName/Folder/file_*.csv'. 
        destination_table_id {str} -- fully qualified bq table id e.g. project_id.dataset.new_tablename

    Keword Arguments:
        schema {list} -- list of bigquery.schema.SchemaField 
            Partial schema is acceptable. 
            Use get_table_schema_from_bq(table_id) or get_table_schema_from_df(df) to enforce existing table schema
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        job_id {str} -- optional argument, use function create_bq_job_id(description=None) to create custom job id for logging
        job_config {dict} -- any other keyowrd argument for bigquery.job.LoadJobConfig

    Returns:
        bigquery.job.LoadJob

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(**job_config)
    job_config.source_format = bigquery.SourceFormat.CSV

    if schema:
        job_config.autodetect = False
        job_config.schema = schema

    load_job = client.load_table_from_uri(
        source_uri, destination_table_id, job_config=job_config, job_id=job_id)
    logging.debug(f"Starting job {load_job.job_id}")
    return load_job


def bq_export_csv_to_gcs(source_table_id, gcs_bucket, client=None, **job_config):
    """Export bigquery table to gcs bucket as CSV. large files will be split into multiple files.

    Arguments:
        source_table_id {str} -- fully qualified bq table id e.g. project_id.dataset.new_tablename
        gcs_bucket {str} -- google cloud storage bucket name

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        job_config {dict} -- keyowrd argument for bigquery.job.ExtractJobConfig

    Returns:
        bigquery.job.ExtractJob

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    TABLE_NAME = source_table_id.split('.')[2]
    # bytes converted to MB for large table > 1000MB
    if client.get_table(source_table_id).num_bytes/1000000 > 1000:
        destination_uri = "gs://{gcs_bucket}/{TABLE_NAME}/{'bigquery_export_*.csv'}"
    else:
        destination_uri = "gs://{gcs_bucket}/{TABLE_NAME}.csv"

    job_config = bigquery.ExtractJobConfig(**job_config)
    extract_job = client.extract_table(
        source_table_id,
        destination_uri,
        job_config=job_config

    )
    logging.info("Started exporting {source_table_id} to {destination_uri}")
    return extract_job


def create_bq_job_id(description=None):
    """Create custom job id for bigquery jobs for logging and using the log as event in yyymmdd_hhmmss_EST_description pattern.

    Arguments:
        description {str} -- description of job. (load job, export, query, DML statements type, app, client id etc)
            use searchable terms to pull the job from logs (default: service_account_email prefix)

    Returns:
        job_id {str} -- custom job Id with yyymmdd_hhmmss_EST_description pattern

    """
    timezone = pytz.timezone("America/New_York")
    utc_now = pytz.utc.localize(datetime.datetime.utcnow())
    est_now_str = utc_now.astimezone(pytz.timezone(
        "America/New_York")).strftime("%Y%m%d_%H%M%S_EST")

    if description:
        description = description.replace(' ', '_')
        job_id = est_now_str + f"_{description}"
    else:
        agent = client.get_service_account_email()  # client from outer scope
        job_id = est_now_str + f"_{agent.split('@')[0]}"
    return job_id


def get_table_schema_from_bq(table_id, client=None, output_option='OBJECT'):
    """Return table schema of Biqquery Table.

    Arguments:
        table_id {str} -- fully qualified table id e.g. project_id.dataset.new_tablename

    Keyword Arguments:
        client {bigquery.Client} -- defaults to client instantiated with default credentials
        output_options {str} -- {'OBJECT','LIST','DICT'} 
            'OBJECT' returns table schema as list of bigquery.schema.SchemaField
            'LIST' returns list of columns names
            'DICT' returns field api_repr as list of dict: field_type. (default: bigquery schema object)

    Returns:
        biqquery schema object|list of str|list of dict containing fields definition

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    if output_option == 'LIST':
        return [field.name for field in client.get_table(table_id).schema]
    elif output_option == 'DICT':
        return [field.to_api_repr() for field in client.get_table(table_id).schema]
    else:
        return client.get_table(table_id).schema


def get_table_schema_from_df(df, bq_dtypes=None, output_options='OBJECT'):
    """Return table schema for Biqquery Table from pandas DataFrame with Bigquery compliant column names.

    Arguments:
        df {pd.DataFrame} -- pandas dataframe

    Keyword Arguments:
        bq_dtypes {dict} -- {"field_name": "field_type"} to over-ride inferred dtype
        output_options {str} -- {'OBJECT','LIST','DICT'} 
            'OBJECT' returns table schema as list of bigquery.schema.SchemaField
            'LIST' returns list of columns names
            'DICT' returns dictionary for each field : field_type. (default: bigquery schema object)

    Returns:
        {list, dict} -- list of bigquery.schema.SchemaField|list of str|list of dict containing fields definition

    Examples:
    # simple invocation
    schema = get_table_schema_from_df(df)

    # passing custom dtype for NESTED field
    get_table_schema_from_df(df,bq_dtypes={'date_obj':'DATETIME','entity_analysis': {'salience': 'STRING'},'document_classification':{'last updated':"DATE" }})

    """
    column_names = list(df.columns)

    invalid_cols = _validate_df_column_names(df, output_option='INVALID')
    if invalid_cols:
        new_column_names = _validate_df_column_names(
            df, output_option='RENAME')
        logging.warning(
            f'{invalid_cols} is invalid field name and was renamed')
        df.columns = new_column_names
    else:
        new_column_names = column_names

    if output_options == 'LIST':
        return [_create_field_schema_api_repr(df[field]).get('name') for field in new_column_names]
    elif output_options == 'DICT':
        return {_create_field_schema_api_repr(df[field]).get('name'): _create_field_schema_api_repr(df[field]).get('type') for field in new_column_names}
    else:
        output = [_create_field_schema_api_repr(
            df[field]) for field in new_column_names]

        if bq_dtypes:
            for field_name, dtype in bq_dtypes.items():
                for field in output:
                    if field.get('name') == field_name:
                        if field.get('type') == 'RECORD':
                            children_fields = field.get('fields')
                            for child_field in children_fields:
                                if child_field.get('name') in dtype.keys():
                                    child_field['type'] = dtype[child_field.get(
                                        'name')].upper()
                        else:
                            field['type'] = dtype.upper()
        return [bigquery.schema.SchemaField.from_api_repr(field) for field in output]


def _create_field_schema_api_repr(series):
    """Create field schema api_repr from pandas.Series (DataFrame column).

    pandas.Series data type is mapped to Bigquery field type with object_regex_class_dict for object dtype series to find NESTED & REPEATED Fields below using regex
    series_regex_dtype_dict is used for non-object dtypes series

    Returns:
        dict -- with ``name``, ``type``, ``description``, `mode``,  and ``fields`` keys

    """
    series_regex_dtype_dict = {
        'bool': 'BOOLEAN',
        'int': 'INTEGER',
        'float': 'FLOAT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'object': 'STRING',
        "string": 'STRING',
    }
    object_regex_class_dict = {
        'bool': 'BOOLEAN',
        'int': 'INTEGER',
        'float': 'FLOAT',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'str': 'STRING',
        "string": 'STRING'
    }

    def dtype_from_object_sample(sample_value):
        """Create key for object dtype series from last non-null value.

        Sample_value is used to infer schema of nest & repreated field

        Arguments:
            sample_value {any} -- last non-null value is used to infer column type with regex

        Returns:
            str -- class key for for column

        """
        class_pattern = re.compile(r"<class '[a-z]*?[.]?([a-z]*)[0-9]*'>")
        datetime_pattern = re.compile(
            r".*date.*|.*(Timestamp)'>", re.IGNORECASE)
        bool_pattern = re.compile("<class '.*\.(bool)_?'>")

        if (re.search(class_pattern, str(type(sample_value)))):
            return re.search(class_pattern, str(type(sample_value))).group(1)
        elif re.search(datetime_pattern, str(type(sample_value))):
            if sample_value.hour == sample_value.minute == sample_value.microsecond == 0:
                return 'date'
            else:
                return 'timestamp'
        elif re.search(bool_pattern, str(type(sample_value))):
            return 'bool'
        else:
            logging.debug(f"{str(type(sample_value))} not implemented")
            return 'str'  # unconfirmed default dtype

    def dtype_from_series_dtype(series):
        """Determine data type for each column from last non-null value.

        Sample_value is used to infer schema of nest & repreated field

        Arguments:
            sample_value {any} -- last non-null value is used to infer column type with regex

        Returns:
            str -- class key for for column

        """
        dtype_pattern = re.compile(r"([a-z]*)[0-9]*")
        dtypes_str = str(series.dtypes)

        if re.search(dtype_pattern, dtypes_str):
            key = re.search(dtype_pattern, dtypes_str).group(1)
            if key == 'datetime':
                if series.dt.freq == 'D':
                    return 'date'
                else:
                    return 'datetime'
            else:
                return key
        else:
            logging.debug(f"{dtypes_str} not implemented")
            return 'str'  # unconfirmed default dtype

    series = series.dropna()

    if isinstance(series, pd.Series) and len(series) > 0:
        if str(series.dtypes) == 'object':
            obj_sample = series.iloc[-1]  # last null values as sample
            if isinstance(obj_sample, list):
                if isinstance(obj_sample[0], dict):
                    logging.debug(
                        f'{series.name} is NESTED and REPEATED field')

                    children_fields = [{'mode': 'NULLABLE', 'name': key, 'type': object_regex_class_dict.get(dtype_from_object_sample(
                        obj_sample[0].get(key)), 'STRING'), 'description': None} for key in list(obj_sample[0].keys())]

                    field_schema_repr = {'mode': 'REPEATED',
                                         'name': series.name,
                                         'type': 'RECORD',
                                         'description': None,
                                         'fields': children_fields}
                    return field_schema_repr
                else:
                    logging.debug(f'{series.name} is REPEATED field')

                    field_schema_repr = {'mode': 'REPEATED',
                                         'name': series.name,
                                         'type': object_regex_class_dict.get(dtype_from_object_sample(obj_sample[0]), 'STRING'),
                                         'description': None
                                         }
                    return field_schema_repr

            elif isinstance(obj_sample, dict):
                logging.debug(f'{series.name} is NESTED field')

                children_fields = [{'mode': 'NULLABLE', 'name': key, 'type': object_regex_class_dict.get(dtype_from_object_sample(
                    obj_sample.get(key)), 'STRING'), 'description': None} for key in list(obj_sample.keys())]
                field_schema_repr = {'mode': 'NULLABLE',
                                     'name': series.name,
                                     'type': 'RECORD',
                                     'description': None,
                                     'fields': children_fields}
                return field_schema_repr
            else:
                field_schema_repr = {'mode': 'NULLABLE',
                                     'name': series.name,
                                     'type': object_regex_class_dict.get(dtype_from_object_sample(obj_sample), 'STRING'),
                                     'description': None
                                     }
                return field_schema_repr
        else:

            field_schema_repr = {'mode': 'NULLABLE',
                                 'name': series.name,
                                 'type': series_regex_dtype_dict.get(dtype_from_series_dtype(series)),
                                 'description': None
                                 }
            return field_schema_repr

    else:
        field_schema_repr = {'mode': 'NULLABLE',
                                     'name': series.name,
                                     'type': 'STRING',
                                     'description': None
                             }
        return field_schema_repr


def _validate_df_column_names(df, output_option='INVALID'):
    """Validate df column names against biquery table name restrictions and return alternative valida names
    Naming restriction are: Fields must contain only letters, numbers, and underscores, start with a letter or underscore, and be at most 128 characters long.

    Arguments:
        df {pandas dataframe}

    Keyword Arguments:
        output_option {str} -- INVALID return list of invalid names | VALID returns list of valid names | RENAME returns new list of valid names by replacing invlid characters with underscore (default: {'INVALID'})

    Returns:
        list -- list of column names

    """

    valid_name_pattern = re.compile(r'(^[^a-zA-Z])|(\W)')
    if output_option == 'INVALID':
        invalid_names = [col for col in list(
            df.columns) if re.search(valid_name_pattern, col)]
        return invalid_names
    elif output_option == 'VALID':
        valid_names = [col for col in list(
            df.columns) if not re.search(valid_name_pattern, col)]
        return valid_names
    else:
        new_col = [valid_name_pattern.sub('_', col)
                   for col in list(df.columns)]
        return new_col


def add_column_to_schema(schema, column_name='Col_index_number', column_type='STRING', column_mode='NULLABLE', column_index=-1, children_fields=None):
    """Add field to schema list.

    Arguments:
        schema {list} -- list of bigquery.schema.SchemaField 

    Keyword Arguments:
        column_name {str} -- bigquery field name (default: {'Col_index_number'})
        column_type {str} -- bigquery data types including 'INTEGER', 'FLOAT', 'DATE', 'TIMESTAMP', 'BOOLEAN', 'RECORD' (default: {'STRING'})
        column_mode {str} -- NULLABLE | REQUIRED | REPEATED (default: {'NULLABLE'})
        column_index {int} -- index position of new column in the field list (default: {-1})
        children_fields {list} -- schema for nested field,  get_table_schema_from_df(df) auto generates nested fields (default: {None})
        list of single dict for NESTED REPEATED columns e.g [{name1: type1, name2: type2, name3: type3}]
        or list of dicts for NESTED REPEATED key:value columns [{name1: type1}, {name2: type2}, {name3: type3}]
        or single_dict of fixed number of keys for NESTED column {name1: type1, name2: type2, name3: type3}
        or list of values for REPEATED columns
        children schema = [list of dict with name and type]
        e.g [{"name": "child_col1", "type": "STRING"}, {"name": "child_col2",
            "type": "INTEGER"},{"name": "child_col3", "type": "BOOLEAN"}]

    Returns:
        None - modifies schema object passed as function argument in place

    Example:

    current_schema = get_table_schema_from_bq(
        'project_id.temptables_dev.case1_nested_repeated_withdate')
    add_column_to_schema(current_schema, column_name ='Simple_Date_Col',
                         column_type='DATE') # add a date column

    # add nested column
    step_children= [{"name": "child_col1", "type": "STRING"}, {
        "name": "child_col2", "type": "INTEGER"},{"name": "child_col3", "type": "BOOLEAN"}]
    add_column_to_schema(current_schema, column_name ='nested_collumn', column_type='RECORD',
                         column_mode='REPEATED', column_index=-2, children_fields=step_children)

    """
    if column_name == 'Col_index_number':
        column_name = f'Col_{str(len(schema)+1)}'

    field_schema_rep = {'mode': column_mode,
                        'name': column_name,
                        'type': column_type,
                        'description': None
                        }

    if children_fields:
        children_fileds_schema = [{'mode': 'NULLABLE', 'name': field.get('name', f'child_col_{index}'), 'type': field.get(
            'type', 'STRING')} for index, field in enumerate(children_fields)]
        field_schema_rep['fields'] = children_fileds_schema
    if column_index == -1:
        column_index = len(schema)

    return schema.insert(column_index, bigquery.schema.SchemaField.from_api_repr(field_schema_rep))


def get_gcp_service_account(credentials, storage_client=None):
    """Return google.oauth2.service_account.Credentials from local filepath, gcs_uri or dict.

    Arguemnts:
        credentials {str, bytes, dict}
    Keyword Arguments:
         storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    Returns:
        google.oauth2.service_account.Credentials

    """
    from google.oauth2.service_account import Credentials
    from pyplatform.datalake import gcs_download_blob
    if isinstance(credentials, dict):
        credentials = Credentials.from_service_account_info(credentials)
        logging.debug(
            f"project_id: {credentials.project_id} service_account_email: {credentials.service_account_email}")
    else:
        if os.path.isfile(credentials):
            credentials = Credentials.from_service_account_file(credentials)
            logging.debug(
                f"project_id: {credentials.project_id} service_account_email: {credentials.service_account_email}")

        elif isinstance(credentials, bytes):
            credentials = Credentials.from_service_account_info(
                json.loads(credentials))
            logging.debug(
                f"project_id: {credentials.project_id} service_account_email: {credentials.service_account_email}")

        elif 'gs://' in credentials:
            if not storage_client:
                from google.cloud import storage
                logging.debug(
                    "instantiating storage client from defualt environment variable")
                storage_client = storage.Client()

            credentials = gcs_download_blob(
                credentials, output_option='CREDENTIALS')  # dependency
            logging.debug(
                f"project_id: {credentials.project_id} service_account_email: {credentials.service_account_email}")
        else:
            credentials = Credentials.from_service_account_info(
                json.loads(credentials))
            logging.debug(
                f"project_id: {credentials.project_id} service_account_email: {credentials.service_account_email}")

    return credentials

# Mostly Admin stuff


def bq_create_dataset(dataset_name='DataSources', client=None):
    """Create bigquery dataset.

    Keyword Arguments:
        dataset_name {str} -- name for dataset (default: {'DataSources'})
        client {bigquery.Client} -- (default: {None})

    Returns:
        str -- dataset_id in project.dataset_name format

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    project_id = client.project
    dataset_id = f"{project_id}.{dataset_name}"
    client.create_dataset(dataset_id, exists_ok='False')
    return dataset_id


def bq_list_datasets(client=None):
    """List dataset in project of bigquery.Client.

    Keyword Arguments:
        client {bigquery.Client} -- (default: {None})

    Returns:
        list -- list of dataset_ids in project.dataset_name format

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    project_id = client.project
    datasets = client.list_datasets(project=project_id)
    return [f"{project_id}.{dataset.dataset_id}" for dataset in datasets]


def bq_list_tables(dataset, client=None, output_option='LIST'):
    """Return list of table_ids in the dataset.

    Arguments:
        dataset {str} -- dataset_name or dataset_id e.g. project.dataset_name

    Keyword Arguments:
        client {bigquery.Client} -- (default: {None})
        output_option {str} -- { 'LIST' , 'DF'} changes output (default: {'LIST'})
            ``DF`` returns pandas.DataFrame with table meta data
            meta data includes: 

                table_id {str},
                table_name {str}
                created {datetime}
                modified {datetime}
                size_MB {float}
                num_rows {int}
                table_type {str}
                table_labels {dict}
                partitioning_type {str}

    Return:
        {list} of table_ids in the dataset

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    if dataset.split('/') == 1:
        dataset = "{client.project}.".join(dataset)

    tables = list(client.list_tables(dataset))
    if tables:
        logging.debug(f"{len(tables)} tables in {dataset}")
        if output_option == 'DF':
            meta_data = []

            for table in tables:
                t = f"{table.project}.{table.dataset_id}.{table.table_id}"
                table = client.get_table(t)
                meta_data.append({"table_id": t, "table_name": table.table_id, "created": table.created, "modified": table.modified, "size_MB": table.num_bytes/1000000, "num_rows": table.num_rows                              # should be nested and repeated
                                  , "table_type": table.table_type, "table_labels": table.labels, "partitioning_type": table.partitioning_type})
            return pd.DataFrame(meta_data)
        else:
            return [f"{client.project}.{dataset}.{table.table_id}" for table in tables]
    else:
        logging.debug(f"{dataset} dataset does not contain any tables.")


def bq_update_table_metadata(table_id, table_description=None, table_labels=None, client=None):
    """Update bigquery table description and labels.

    Arguments:
        table_id {str} -- fully qualified table_id of target table e.g. project_id.dataset.new_tablename

    Keyword Arguments:
        table_description {str} -- text for description of table
        table_labels {dict} -- containing key:value e.g. {"env":"prod"}
        client {bigquery.Client} (default: {None})

    Returns:
        bigquery.Table

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    table = client.get_table(table_id)

    if table_description:
        table.description = table_description
        table = client.update_table(table, ["description"])
        logging.debug(f"Updated {table_id} description")
    if table_labels:
        table.labels = table_labels
        table = client.update_table(table, ["labels"])
        logging.debug(f"Labels added to {table_id}")
    return table


def bq_create_table(table_id, schema, partition_column_name=None, cluster_column_name=None, if_exists='ERROR', client=None):
    """Create bigquery table with paritioned and clustering columns.

    Arguments:
        table_id {str} -- fully qualified table_id e.g. project_id.dataset.new_tablename
        schema {bigquery} -- fully qualified table_id e.g. project_id.dataset.new_tablename

    Keyword Arguments:
        partition_column_name {str} -- optional. column must be of date, timestamp or integer type
        cluster_column_name {list} -- list of column names. e.g. ['column1','column2','column3','column4']
        if_exists {str}: {'ERROR','REPLACE','IGNORE'} If ``REPLACE`` deletes existing table and creates new one, if ``IGNORE`` doesn't raise error if table already exists (default: {'ERROR'})
        client {bigquery.Client} -- (default: {None})

    Returns:
        bigquery.Table

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    if if_exists == 'REPLACE':
        client.delete_table(table_id, not_found_ok=True)
        logging.debug(f"deleted {table_id}")
        exists_ok = True
    elif if_exists == 'IGNORE':
        exists_ok = True
    else:
        exists_ok = False

    table = bigquery.Table(table_id, schema=schema)
    if partition_column_name:
        table.time_partitioning = bigquery.TimePartitioning(
            field=partition_column_name)
        logging.debug(
            f"{table.table_id} will be partitioned on column {table.time_partitioning.field}")
    if cluster_column_name:
        table.clustering_fields = cluster_column_name
        logging.debug(
            f"{table.table_id} will be clustered on columns {table.clustering_fields}")
    try:
        client.create_table(table, exists_ok=exists_ok)
        logging.debug(
            f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
        return table
    except Exception as error:
        error  # TODO


def bq_copy_table(source_table_id, destination_table_id, write_mode='WRITE_EMPTY', job_id=None, client=None, **job_config):
    """Copy source bigquery table.

    Arguments:
        source_table_id {str} -- fully qualified bq table id of source table e.g. project_id.dataset.new_tablename
        destination_table_id {str} -- fully qualified bq table id of destination table e.g. project_id.dataset.new_tablename

    Keyword Arguments:
        write_mode {str} -- {'WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'}
        job_id {str} -- use function create_bq_job_id(description=None) to create custom job id
        client {bigquery.Client} -- (default: {None})
        job_config {dict} -- keyword arguments for bigquery.CopyJobConfig

    Returns:
        bigquery.job.CopyJob

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    job_config = bigquery.bigquery.CopyJobConfig(**job_config)
    job_config.write_disposition = write_mode
    job = client.copy_table(
        source_table_id, destination_table_id, job_config=job_config, job_id=job_id)

    return job


def bq_del_table(table_id, client=None):
    """Delete bigqquery table.

    Arguments:
    table_id {str} -- fully qualified id of target table e.g. project_id.dataset_name.table_name

    Keyword Arguments:
        client {bigquery.Client} -- (default: {None})

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    client.delete_table(table_id)
    logging.debug(f'{table_id} was deleted')


def bq_restore_table(table_id, snapshot_datetime=None, client=None):
    """Restore bigquery table to a previous snapshot or recover deleted table.

        copy of existing table can be recovered within 7 days
        deleted table can be restored with 2 days of deletion
        recovered table is suffixed with timestamp_restored e.g. project_id.dataset.recovered_table_name_timestamp_restored

    Arguments:
        table_id {str} -- fully qualified id of modified/deleted table e.g. project_id.dataset.table_name

    Keyword Arguments:
        snapshot_datetime {str} -- datetime string in "%Y-%m-%dT%H:%M:%S.%fEST" format to milliseconds resolution and EST:America/New_York timezone. DEFAULTS to 2 hours before current EST.
            example1: snapshot_datetime= "2020-01-22T14:00:00.000EST"
            example2: snapshot_datetime= "2019-12-31T14:11:17.651EST"
        client {bigquery.Client} -- (default: {None})

    """
    if not client:
        logging.debug(
            "instantiating bigquery client from defualt environment variable")
        client = bigquery.Client()

    if snapshot_datetime:

        datetime_datetime = datetime.datetime.strptime(
            snapshot_datetime, "%Y-%m-%dT%H:%M:%S.%fEST").astimezone(tz=pytz.timezone('America/New_York'))
        snapshot_epoch = int(datetime.datetime.timestamp(
            datetime_datetime)*1000)  # milliseconds
        str_datetime = datetime_datetime.strftime("%Y%m%d_%H%M%S_EST")
        logging.debug(
            f"provided snapshot timestamp: {datetime_datetime} EST <=> unix_millis:{snapshot_epoch}")
    else:
        datetime_datetime = datetime.datetime.now(tz=pytz.timezone(
            'America/New_York')) + datetime.timedelta(hours=-2)
        snapshot_epoch = int(datetime.datetime.timestamp(
            datetime_datetime) * 1000)  # 2 hours in milliseconds
        str_datetime = datetime_datetime.strftime("%Y%m%d_%H%M%S_EST")
        logging.debug(
            f"2 hours ago snapshot timestamp: {datetime_datetime} EST <=> unix_millis:{snapshot_epoch}")

    snapshot_table_id = f"{table_id}@{snapshot_epoch}"
    restored_table_id = table_id+'_'+str_datetime+'_restored'

    job = client.copy_table(
        snapshot_table_id,
        restored_table_id,
        location="US",
    )
    try:
        job.result()
        logging.debug(f"{table_id} was restored as {restored_table_id}")
    except Exception as err:
        logging.debug(str(err.message))
