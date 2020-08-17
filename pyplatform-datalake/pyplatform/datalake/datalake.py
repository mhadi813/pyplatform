import os
import io
import logging
import json
import re
import pandas as pd
import datetime
import pytz
from google.cloud import storage
from azure.storage.blob.blockblobservice import BlockBlobService
# TODO testing


def gcs_list_buckets(storage_client=None):
    """Return all bucket_ids as list.

    Keyword Arguments:
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    Returns:
        {list} of str -- bucket_id (google.cloud.storage.bucket.Bucket)

    """
    if not storage_client:
        logging.debug(
            "instantiating storage client from defualt environment variable")
        storage_client = storage.Client()

    list_of_buckets = [bucket.name for bucket in storage_client.list_buckets()]

    logging.debug(
        f"{len(list_of_buckets)} buckets found in {storage_client.project}")
    return list_of_buckets


def gcs_list_blobs(bucket_id=None, storage_client=None, folder_name=None, file_extenion='.csv'):
    """Return lists of blobs from bucket.

    Keyword Arguments:
        bucket_id {str} -- google cloud storage bucket id (default: default bucket from the project)
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})
        folder_name {str} -- google cloud storage folder (i.e. blob prefix) e.g. in uri gs://bucket_id/folder_name/blob_name (default: None)
        file_extenion {str} -- e.g. csv, xlsx , png etc (default: {'.csv'})

    Returns:
        list -- list of blob matching folder_name and file_extension criteria

    """
    if not bucket_id:
        bucket_id = os.environ.get("STORAGE_BUCKET")
    if not folder_name:
        folder_name = "."
    if not file_extenion:
        file_extenion = ".$"
    else:
        file_extenion = file_extenion.lower()
    blobs = storage_client.list_blobs(bucket_id)
    files = []
    for blob in blobs:
        files.append(blob.name)

    files = [x for x in files if bool(
        re.search(r'{}.+{}'.format(folder_name, file_extenion), x))]
    if folder_name == ".":
        logging.debug(f'{len(files)} objects found in {bucket_id} bucket')
    else:
        logging.debug(
            f'{len(files)} objects found in {bucket_id}/{folder_name} folder')
    return files


def gcs_copy_blob(bucket_id, blob_name,  new_bucket_id=None, new_blob_name=None, storage_client=None):
    """Create a new copy of blob or copies a blob from one bucket to another bucket. copied blob can be renamed

    Arguments:
        bucket_id {str} -- google cloud storage bucket id of target blob
        blob_name {str} -- name target blob e.g. please_copy_me.txt

    Keyword Arguments:
        new_bucket_id {str} -- destination bucket id (default: same_bucket)
        new_blob_name {str} -- new name for blob. provide blob prefix to move blob to a folder e.g. new_folder/random_file.csv (default: copy_original_file)
        storage_client {storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    """
    if not new_bucket_id:
        new_bucket_id = bucket_id
        if "/" in blob_name:  # remove blob prefix (folder name)
            new_blob_name = f"copy_{blob_name.split('/')[-1]}"
        else:
            new_blob_name = f"copy_{blob_name}"
    else:
        if not new_blob_name:
            if "/" in blob_name:  # remove blob prefix (folder name)
                new_blob_name = blob_name.split("/")[-1]
            else:
                new_blob_name = blob_name

    source_bucket = storage_client.get_bucket(bucket_id)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_id)

    source_bucket.copy_blob(source_blob, destination_bucket, new_blob_name)
    logging.info(
        "gs://{bucket_id}/{blob_name} was copied to gs://{new_bucket_id}/{new_blob_name}")


def gcs_rename_blob(bucket_id, blob_name, new_blob_name, storage_client=None):
    """Rename blob, if new_blob_name contains blob prefix, blob is "moved" to folder.

    Arguments:
        bucket_id {str} -- google cloud storage bucket id
        blob_name {str} -- google cloud storage blob_name e.g. random_file.csv
        new_blob_name {str} -- new name for blob. provide blob prefix to move blob to a folder e.g. new_folder/random_file.csv

    Keyword Arguments:
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    """
    bucket = storage_client.get_bucket(bucket_id)
    blob = bucket.blob(blob_name)

    new_blob = bucket.rename_blob(blob, new_blob_name)
    if "/" in new_blob_name:
        logging.info(
            f'Blob {blob.name} has been moved to {new_blob_name.split("/")[0]} folder')
    else:
        logging.info(f'Blob {blob.name} has been renamed to {new_blob.name}')


def gcs_download_blob(gcs_uri, filepath=None, storage_client=None, output_option='FILE', delete_blob=False):
    """Downloads blob from Google Cloud Storage to local drive or buffer.

    Arguments:
        gcs_uri {str} -- google cloud storage blob uri e.g. gs://bucketname/foldername/blobname.csv

    Keyword Arguments:
        filepath {str} -- folder_path/new_filename (default: current_working_directory/blob_name)
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})
        output_option {str} -- determines function output type. (default: {'FILE'})
        delete_blob {bool} -- flag for deleting the blob after download is successful (default: {False})

    Returns:
        output_option : return type
        {STRING : unicode string}
        {IO : io.BytesIO}
        {URL: downloadable link}
        {CREDENTIALS: google.oauth2.service_account.Credentials}
        {JSON: DICT}
        {FILE: filepath}
    """
    from google.oauth2.service_account import Credentials

    if not storage_client:
        logging.debug(
            "instantiating storage client from defualt environment variable")
        storage_client = storage.Client()

    bucket = gcs_uri[5:].split('/')[0]
    blob = '/'.join(gcs_uri[5:].split('/')[1:])
    blob = storage_client.get_bucket(bucket).get_blob(blob)

    logging.debug(
        f"storage client {storage_client.get_service_account_email()} is requesting access to blob: {blob} at {gcs_uri} ")

    if output_option == 'IO':
        output = io.BytesIO()
        blob.download_to_file(output)
        output.seek(0)

    elif output_option == 'STRING':
        output = blob.download_as_string().decode()

    elif output_option == 'JSON':
        output = json.loads(blob.download_as_string().decode())

    elif output_option == 'CREDENTIALS':
        output = Credentials.from_service_account_info(
            json.loads(blob.download_as_string().decode()))

    elif output_option == 'URL':
        expiration_duration = datetime.timedelta(weeks=1)
        output = blob.generate_signed_url(expiration_duration)

    else:
        if not filepath:
            filepath = gcs_uri.split("/")[-1]
        with open(filepath, mode="wb") as file_obj:
            storage_client.download_blob_to_file(gcs_uri, file_obj)
        output = filepath

    if delete_blob:
        blob.delete()

    return output


def gcs_upload_blob(content, bucket_id=None, blobname=None, storage_client=None):
    """Uploads content to Google Cloud Storage.

    Arguments:
        content {str, filepath, io.BytesIO} -- string, filepath or BytesIO to be uploaded

    Keyword Arguments:
        bucket_id {str} -- google cloud storage bucket id (default: default bucket from env variable "STORAGE_BUCKET")
        blobname {str} -- blob name, for string and BytesIO name should be provided (default: {unname_blob_uploaded_at_timestamp})
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    Return:
        {str} -- returns google cloud storage uri of uploaded blob
    """

    if not storage_client:
        logging.debug(
            "instantiating storage client from defualt environment variable")
        storage_client = storage.Client()

    if not bucket_id:
        bucket_id = os.environ.get("STORAGE_BUCKET")

    if not blobname:
        if os.path.isfile(content):
            blobname = os.path.basename(content)
            storage_client.get_bucket(bucket_id).blob(
                blobname).upload_from_filename(content)
            return f'gs://{bucket_id}/{blobname}'
        else:
            blobname = f"unname_blob_uploaded_at{datetime.datetime.now().isoformat().replace('-', '_').replace(':', '_')[:19]}"

    if isinstance(content, io.BytesIO):
        storage_client.get_bucket(bucket_id).blob(
            blobname).upload_from_file(content)
    else:
        storage_client.get_bucket(bucket_id).blob(
            blobname).upload_from_string(content)

    return f'gs://{bucket_id}/{blobname}'


def gcs_upload_folder(bucket_id=None, folderpath="./", output=True, storage_client=None):
    """uploads local folder to google cloud storage

    Keyword Arguments:
        bucket_id {str} -- google cloud storage bucket id (default: default bucket from the project)
        folderpath {str} -- relative or absolute path of directory (default: {"./"})
        output {bool} -- if True returns upload files' uri (default: True) as list
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    Return:
        {str|None} -- if output is True, return list of google cloud storage uris

    """
    if not storage_client:
        logging.debug(
            "instantiating storage client from defualt environment variable")
        storage_client = storage.Client()

    if not bucket_id:
        bucket_id = os.environ.get("STORAGE_BUCKET")

    bucket = storage_client.bucket(bucket_id)

    if folderpath == "./":
        folderpath = os.path.abspath('.')
        folder = folderpath.split('/')[-1]
    else:
        folder = folderpath.split('/')[-1]

    local_files = []
    for (dirpath, dirnames, filenames) in os.walk(folderpath):
        for file in filenames:
            local_files.append(os.path.join(dirpath, file))

    gcs_path = [folder+'/' + file.replace(folderpath+'/', '')
                for file in local_files]

    for local, gcs in zip(local_files, gcs_path):
        if os.path.isfile(local):
            bucket.blob(gcs).upload_from_filename(local)
            logging.info(f'uploaded : {gcs}')
    if output:
        return gcs_path


def gcs_del_storage_folder(bucket_id, folder=None, blob_name=None, storage_client=None):
    """Delete  blobs from GCP Storage.

    Arguments:
        bucket_id {str} -- gcs bucket name

    Keyword Arguments:
        folder {str} -- gcs blob prefix or folder containing the target files
            if folder is provided, all blobs in the folder will be deleted.
        blob_name {str} -- blob name or individual file name
            if only blob name is provided, only that single blob will be deleted
        storage_client {google.storage.Client} -- storage client instantiated with non-default credentials (default: {None})

    """
    if not storage_client:
        logging.debug(
            "instantiating storage client from defualt environment variable")
        storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_id)

    if blob_name:
        blob = bucket.blob(blob_name)
        try:
            blob.delete()
            logging.info("Blob {} deleted.".format(blob_name))
        except Exception as err:
            logging.info(str(err.message))
        return

    if folder:
        try:
            bucket.delete_blobs(blobs=bucket.list_blobs(prefix=folder))
        except Exception as err:
            logging.error(str(err.message))


def azure_get_credentials(**kwargs):
    """ infers and/or updates defaults for Azure SQL and Azure storage account
    defaults comes from credentials.json filepath set as environment 'AZURE_CREDENTIALS'

    Keyword Arguments:
        account_name {str} 'https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}'
        container_name {str} - storage container name
        account_key {str} - account_key for storage authentication

        server {str} -- azure sql server url e.g "myazuresql.database.windows.net"
        username {str} -- user id (default: admin )
        password {str} -- password (default: admin_password)
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
            'default credentials path does not exit, no defaults set for tableau server authentication')
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

# updated name


def azure_storage_download_blob(blob_url, account_key=None, filepath=None, output_option='FILE', delete_blob=False):
    """ downloads azure blob to FILE, io.BytesIO, STRING, JSON STRING type

    Arguments:
        blob_url {str} -- azure storage blob url e.g https://account_name.blob.core.windows.net/container_name/blob_name
        account_key {str} -- authentication method for azure storage

    Keyword Arguments:
        filepath {str} -- folder_path/new_filename (default: current_working_directory/blob_name)
        output_option {str} -- determines function output type. (default: {'FILE'})
        delete_blob {bool} -- flag for deleting the blob after download is successful (default: {False})

    Returns:
        Returns:
        output_option : return type
        {STRING : unicode string}
        {IO : io.BytesIO}
        {URL: downloadable link}
        {JSON: DICT}
        {FILE: filepath}

    """
    from io import BytesIO

    account_name, container_name, blob_name = __parse_storage_url(blob_url)
    if not account_key:
        account_key = azure_get_credentials().get('account_key')  # dependency

    service = BlockBlobService(
        account_name=account_name, account_key=account_key)
    blob = service.get_blob_to_bytes(container_name, blob_name)

    if output_option == 'IO':
        output = BytesIO(blob.content)
        output.seek(0)

    elif output_option == 'STRING':
        output = blob.content.decode()

    elif output_option == 'JSON':
        output = json.loads(blob.content.decode())

    elif output_option == 'URL':
        from datetime import datetime, timedelta
        from azure.storage.blob import BlobPermissions
        from urllib.parse import quote

        sas_token = service.generate_blob_shared_access_signature(
            container_name=container_name, blob_name=blob_name, permission=BlobPermissions(
                read=True),
            expiry=datetime.utcnow() + timedelta(weeks=1)
        )
        output = service.make_blob_url(
            container_name, quote(blob_name), sas_token=sas_token)

    else:
        if not filepath:
            filepath = blob_name
        with open(filepath, mode='wb') as file:
            file.write(blob.content)
        output = filepath

    if delete_blob:
        service.delete_blob(container_name, blob_name)

    return output


def azure_storage_upload_blob(content, blob_name=None, **credentials):
    """Uploads content to Azure Storage

    Arguments:
        content {str, filepath, bytes, io.BytesIO} -- string, bytes, filepath or BytesIO to be uploaded


    Keyword Arguments:
        blob_name {str} -- blob name, for string and BytesIO name should be provided (default: {unname_blob_uploaded_at_timestamp})

    Returns:
        {str} -- returns Azure storage url of uploaded blob
    """
    import io
    from datetime import datetime
    credentials = azure_get_credentials(**credentials)  # dependency

    account_name = credentials.get('account_name')
    container_name = credentials.get('container_name')
    account_key = credentials.get('account_key')

    service = BlockBlobService(
        account_name=account_name, account_key=account_key)

    if not blob_name:
        if os.path.isfile(content):
            blob_name = os.path.basename(content)
            service.create_blob_from_path(container_name, blob_name, content)
            return __make_storage_url(account_name, container_name, blob_name)
        else:
            blob_name = 'unname_blob_uploaded_at' + datetime.now().isoformat().replace('-',
                                                                                       '_').replace(':', '_')[:19]

    if isinstance(content, io.BytesIO):
        service.create_blob_from_stream(container_name, blob_name, content)
    elif isinstance(content, bytes):
        service.create_blob_from_bytes(container_name, blob_name, content)
    else:
        service.create_blob_from_text(container_name, blob_name, content)
    return __make_storage_url(account_name, container_name, blob_name)


def __parse_storage_url(blob_url):
    account_name, container_name, blob_name = blob_url[8:].split('/')
    account_name = account_name.split('.')[0]
    return account_name, container_name, blob_name


def __make_storage_url(account_name, container_name, blob_name):
    return f'https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}'
