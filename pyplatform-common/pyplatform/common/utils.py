
import os
import json
import logging
import requests
import jwt
import time
from oauth2client.client import GoogleCredentials
import base64


def credhub_get_credentials(name=None, output_option='FILE'):
    """Return credhub managed credentials  in pivotal cloud  at runtime from `VCAP_SERVICES` environment variable.

    documentatioin: https://docs.pivotal.io/credhub-service-broker/using.html

    Keyword Arguments:
        name {str, list} -- Credhub credential ``Instance Name``  or list of instance names. If not provided, the function attampts to retrieve all credentials (default: None)
        output_option {str,dict} -- output format of credentials.
                {{'key': 'env_variable'}: 'downloads credential ``value`` to a temp file, sets ``env_variable`` for each credential ``key`` pointing to the temp file path' \
                'FILE':'downloads credential ``value`` as a temp file, sets credential ``key`` as variable pointing to temp file path'\
                'DICT':'returns credential ``value`` as dict object'}. (default: {'FILE'})

        Recommended credential keys for each platform::
            {'gpc':'GOOGLE_APPLICATION_CREDENTIALS'
                ,'azure': 'AZURE_CREDENTIALS' 
                , 'pivotal':'PIVOTAL_CREDENTIALS'
                ,'tableau','TABLEAU_SERVER_CREDENTIALS' }

    Returns:
        {str, dict} -- filepath as str and dict as value of credentials
    """
    service = os.environ.get('VCAP_SERVICES')
    if service:
        env = json.loads(service)
        if name:
            if isinstance(name, str):
                cred_name = [cred.get('instance_name') for cred in env.get(
                    'credhub') if cred.get('instance_name') == name]
            else:
                cred_name = [cred.get('instance_name') for cred in env.get(
                    'credhub') if cred.get('instance_name') in name]
        else:
            cred_name = [cred.get('instance_name')
                         for cred in env.get('credhub')]

        logging.debug(f"names of available credentials: {cred_name}")
    else:
        logging.error(f"cred hub or VCAP_SERVICES is not available")
        return

    if cred_name:
        temp_files = []
        creds = {}
        credhub = [cred.get('credentials') for cred in env.get(
            'credhub') if cred.get('instance_name') in cred_name]

        if isinstance(output_option, dict):
            env_mapping = list(output_option.items())

        for i, name in enumerate(cred_name):

            key, value = zip(*credhub[i].items())

            key, value = key[0], value[0]

            if output_option != 'DICT':
                if not os.path.exists('temp'):
                    os.mkdir('temp')
                temp_cred = os.path.join(
                    'temp', f'{name}_temp_credentials.json')

                with open(temp_cred, 'w') as file:
                    file.write(value)

                if isinstance(output_option, dict):
                    try:
                        os.environ[env_mapping[i][1]] = temp_cred
                        temp_files.append(temp_cred)
                    except:
                        os.environ[key] = temp_cred
                        temp_files.append(temp_cred)
                else:
                    os.environ[key] = temp_cred
                    temp_files.append(temp_cred)

            else:
                if isinstance(value, str):
                    try:
                        creds[key] = json.loads(value)
                    except:
                        logging.warning(
                            f"{name} is invalid json, passing on as string")
                        creds[key] = value
                else:
                    creds[key] = value

        if temp_files:
            return temp_files
        else:
            return creds
    else:
        logging.debug(f"no credentails set in credhub")


def gcp_get_auth_header(scope='https://www.googleapis.com/auth/cloud-platform', output_option='AUTH_HEADER', credentials=None):
    """Return OAuth2 Authorization header for google api calls.

    Keyword Arguments:
        scope {str} -- authorization scope (default: {'https://www.googleapis.com/auth/cloud-platform'})
            cloud_function_scope ='https://www.googleapis.com/auth/cloud-platform' or
            cloud_function_scope = 'https://www.googleapis.com/auth/cloudfunctions'
            cloud_storage_scope = 'https://www.googleapis.com/auth/devstorage.read_only'
        output_option {str} -- output format of function (default: {'AUTH_HEADER'})
            other options include {'TOKEN','CREDENTIALS','JSON'}
        credentials {str,dict,path} -- file path of google service account, dict or JSON string contianing credentails (default: {None})

    Returns:
        {str, dict, oauth2client.client.GoogleCredentials, requests.response.json} -- AUTH_HEADER and TOKEN are str
    """

    if not credentials:
        credentials = GoogleCredentials.get_application_default().create_scoped(scope)
        logging.debug(
            f"credentials file not provided, using default credentials {credentials.service_account_email}")

    elif isinstance(credentials, str):
        if os.path.isfile(credentials):
            credentials = GoogleCredentials.from_stream(
                credentials).create_scoped(scope)
            logging.debug(
                f"using service account {credentials.service_account_email}")
        else:
            credentials = json.loads(credentials)
            logging.debug(
                f"using service account {credentials.get('client_email')}")
    elif isinstance(credentials, dict):
        logging.debug(
            f"using service account {credentials.get('client_email')}")
    else:
        logging.error(
            "credential file not provided nor env variable GOOGLE_APPLICATION_CREDENTIALS is set to cred path")
        return

    if isinstance(credentials, GoogleCredentials):
        if output_option == 'TOKEN':
            return credentials.get_access_token().access_token
        elif output_option == 'CREDENTIALS':
            return credentials
        elif output_option == 'JSON':
            access_token = credentials.get_access_token()
            return {"token": access_token.access_token, "expires_in": access_token.expires_in}
        else:
            return f'Bearer {credentials.get_access_token().access_token}'
    else:

        client_secret = credentials['private_key']
        client_email = credentials['client_email']
        # "https://oauth2.googleapis.com/token"
        token_uri = credentials['token_uri']

        iat = time.time()
        exp = iat + 60*60  # Token must be a short-lived token (60 minutes)
        payload = {'iss': client_email,
                   'sub': client_email,
                   'scope': scope,
                   'aud': token_uri,
                   'iat': iat,
                   'exp': exp}
        additional_headers = {'kid': client_secret}
        signed_jwt = jwt.encode(payload, client_secret, headers=additional_headers,
                                algorithm='RS256')
        if output_option == 'JWT':
            return signed_jwt

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": signed_jwt}
        response = requests.request(
            "POST", token_uri, headers=headers, data=data)
        if response.status_code == 200:
            access_token = response.json()['access_token']
            if output_option == 'TOKEN':
                return access_token
            elif output_option == 'JSON':
                return response.json()
            else:
                return f'Bearer {access_token}'
        else:
            logging.error(
                f"access_token request unsuccessfull with status code {response.status_code}")
            return response


def secretmanager_access_secrets(name, project_id=None, output_option='DICT', credentials=None):
    """Return dict with secret ``name`` as key and ``data`` and value or Set ``secret`` name from Google Secret Manager as environment variable.

    documentatioin: https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/access

    Arguments:
        name {str,list} -- name or list of names of secrets

    Keyword Arguments:
        project_id {str} -- project_id hosting the secrets (default: {None})
        output_option {str,dict} -- output format of secrets.
                {{'name': 'env_variable'}: 'downloads secrets ``data`` to a temp file, sets ``env_variable`` for each secret ``name`` pointing to the temp file path' \
                'FILE':'downloads secrets ``data`` to a temp file, sets credential ``name`` as env variable pointing to the temp file path' \
                'DICT':'returns secrets as dict with ``name`` as key and ``data`` as value'}. (default: {'DICT'})

        Recommended secret names for each platform:
            {'gpc':'GOOGLE_APPLICATION_CREDENTIALS'
                ,'azure': 'AZURE_CREDENTIALS' 
                , 'pivotal':'PIVOTAL_CREDENTIALS'
                ,'tableau','TABLEAU_SERVER_CREDENTIALS' }
        credentials {str,dict,path} -- file path of google service account, dict or JSON string contianing credentails (default: {None})

    Returns:
        {str, dict} -- filepath as str and dict as value of credentials

    """

    if credentials:
        auth_header = gcp_get_auth_header(
            scope='https://www.googleapis.com/auth/cloud-platform', credentials=credentials)
    else:
        auth_header = gcp_get_auth_header(
            scope='https://www.googleapis.com/auth/cloud-platform')

    headers = {'Authorization': auth_header,
               'Content-type': 'Application/json', 'Accept': 'Application/json'}

    if not project_id:
        try:
            project_id = os.environ['PROJECT_ID']
        except:
            logging.error(
                'PROJECT_ID env varialbe is not set nor project_id argument was provided to the function')
            return

    if not name:
        url = f"https://secretmanager.googleapis.com/v1/projects/{project_id}/secrets"
        response = requests.request('GET', url, headers=headers)
        if response.status_code == 200:
            names = [n.get('name').split('/')[-1]
                     for n in response.json().get('secrets')]
            logging.debug(f"{names} are available in {project_id}")
        else:
            logging.error(
                f"failed to list all secrets in {project_id}, please provide secret's name to the function")
            return

    else:
        if isinstance(name, str):
            names = [name]
        else:
            assert isinstance(name, list)
            names = name

    if names:
        temp_files = []
        secrets = {}
        if isinstance(output_option, dict):
            env_mapping = list(output_option.items())
        for i, name in enumerate(names):
            url = f"https://secretmanager.googleapis.com/v1/projects/{project_id}/secrets/{name}/versions/latest:access"
            response = requests.request('GET', url, headers=headers)
            if response.status_code == 200:
                data_64 = response.json()['payload']['data']
                value = base64.b64decode(data_64)

                if output_option == 'FILE':
                    if not os.path.exists('temp'):
                        os.mkdir('temp')
                    temp_cred = os.path.join(
                        'temp', f'{name}_temp_credentials.json')

                    with open(temp_cred, 'wb') as file:
                        file.write(value)

                    if isinstance(output_option, dict):
                        try:
                            os.environ[env_mapping[i][1]] = temp_cred
                            temp_files.append(temp_cred)
                        except:
                            os.environ[name] = temp_cred
                            temp_files.append(temp_cred)
                    else:
                        os.environ[name] = temp_cred
                        temp_files.append(temp_cred)
                else:
                    try:
                        secrets[name] = json.loads(value)
                    except:
                        logging.warning(
                            f"{name} is invalid json, passing on as string")
                        secrets[name] = value
            else:
                logging.error(
                    f"failed to access {name} secret with response status code: {response.status_code}")
                logging.debug(f"error message: {response.text}")
        if temp_files:
            return temp_files
        else:
            return secrets
    else:
        logging.debug(f"no secrets available")


def gcs_reader(gcs_uri, auth_header=None, output_option='TEXT'):
    """GET request to Google Cloud Storage api with core Python libraries.

    Arguments:
        gcs_uri {str} -- google cloud storage blob uri e.g. gs://bucketname/foldername/blobname.csv

    Keyword Arguments:
        auth_header {str} -- Bear Authoration token (default: {None})
        output_option {str} -- format of output (default: {'TEXT'})
            other options include {'RESPONSE','CONTENT','JSON','TEXT'}

    Returns:
        {str, requests.response, requests.response.content, requests.response.json, requests.response.text} 
    """
    from urllib.parse import quote
    if not auth_header:
        auth_header = gcp_get_auth_header(scope='https://www.googleapis.com/auth/devstorage.read_only')  # dependency

    bucket = gcs_uri[5:].split('/')[0]
    blob = quote('/'.join(gcs_uri[5:].split('/')[1:]),safe='')

    url = f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{blob}'

    headers = {
        'Authorization': auth_header,
        'content-type': 'application/json'
    }
    response = requests.get(url, headers=headers, params={'alt': 'media'})
    logging.debug(
        f"response code from {response.url} is {response.status_code}")

    if output_option == 'RESPONSE':
        return response
    elif output_option == 'CONTENT':
        return response.content
    elif output_option == 'JSON':
        return response.json()
    else:
        return response.text


def make_requirements_txt(project_dir='.'):
    """Make requirements.txt file from main.py or module folder.

    Keyword Arguments:
        project_dir {str} -- path to main.py or project folder (default: {'current directory'})
            Note: project folder must end with / (mac/linux) or \ for Windows
    """
    if project_dir == '.':
        project_dir = os.path.curdir
    else:
        project_dir = os.path.dirname(project_dir)
#         os.chdir(script_path)
    return os.system(f"pipreqs {project_dir}")

def gcf_authenticated_trigger(url, timeout=0.1):
    """Trigger Google Cloud Function with authenticated GET request
    Note: this will only work in Google Cloud environment (not local machines)

    Arguments:
        url {str} -- url of other gcf e.g.  'https://us-central1-custom-ground-236517.cloudfunctions.net/hello_protected'

    Keyword Arguments:
        timeout {float} -- response timeout in second (default: {0.1} doesn't wait for response)

    Returns:
        flask.response -- retruned from target function if timeout is long enough
    """

    from requests import get, exceptions
    metadata_server_token_url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience='

    token_request_url = metadata_server_token_url + url
    token_request_headers = {'Metadata-Flavor': 'Google'}
    token_response = get(
        token_request_url, headers=token_request_headers)
    jwt = token_response.content.decode("utf-8")

    # Provide the token in the request to the receiving function
    auth_headers = {'Authorization': f'bearer {jwt}'}
    try:
        function_response = get(
            url, headers=auth_headers, timeout=timeout)
        return function_response.content
    except exceptions.ReadTimeout:
        print(f'triggered gcf: {url}')
        return f'triggered gcf: {url}'