import os
import json
import requests
import logging


def cf_get_credentails(**kwargs):
    """Infer and/or update defaults for Pivotal Cloud.
    defaults comes from credentials.json filepath set as environment variable 'PIVOTAL_CREDENTIALS'

    Keyword Arguments:
        username {str} -- user id (default: None)
        password {str} -- password (default: None)
        hostname {str} -- in 'https://api.sys.domainname.com' format
        org {str} -- Pivotal Cloud org name
        token_url {str} -- url for fetching access_token in 'https://login.sys.domain.com/oauth/token' format
        space {str} -- Pivotal Cloud space

    Returns:
    {dict} - credential dictionary
    """
    default_credpath = os.environ.get('PIVOTAL_CREDENTIALS')

    if default_credpath:
        with open(default_credpath, 'r') as file:
            credentials = json.load(file)
    else:
        logging.warning(
            'default credentials path does not exit, no defaults set for tableau server authentication')
        credentials = {}

    if 'username' in kwargs:
        credentials['username'] = kwargs['username']

    if 'password' in kwargs:
        credentials['password'] = kwargs['password']

    if 'hostname' in kwargs:
        credentials['hostname'] = kwargs['hostname']

    if 'org' in kwargs:
        credentials['org'] = kwargs['org']

    if 'token_url' in kwargs:
        credentials['token_url'] = kwargs['token_url']

    if 'space' in kwargs:
        credentials['space'] = kwargs['space']

    return credentials


def cf_get_auth_header(output_option='AUTH_HEADER', **credentials):
    """Return OAuth2 Bearer Access token for Authorization header for Pivotal Cloud Foundary API calls.

    Keyword Arguments:
        output_option {str} -- output format of function (default: {'AUTH_HEADER'})
            other options include {'TOKEN','RESPONSE','JSON'}

        Following keyword arugments can be provided to update default credentials:
        username {str} -- user id (default: None)
        password {str} -- password (default: None)
        hostname {str} -- in 'https://api.sys.domainname.com' format
        org {str} -- Pivotal Cloud org name
        token_url {str} -- url for fetching access_token in 'https://login.sys.domain.com/oauth/token' format
        space {str} -- Pivotal Cloud space

    Returns:
        {str, requests.response, requests.response.json} -- AUTH_HEADER and TOKEN are str
    """

    credentials = cf_get_credentails(**credentials)

    headers = {"Content-Type": "application/json",
               "Accept": "application/json;charset=UTF-8",
               "Authorization": "Basic Y2Y6"}
    username = credentials.get('username')
    password = credentials.get('password')
    token_url = credentials.get('token_url')

    logging.info(f"signing in {token_url} as {username}")
    token_url = f'{token_url}?grant_type=password&username={username}&password={password}&response_type=token'
    response = requests.get(token_url, verify=False, headers=headers)
    logging.debug(
        f"response code: {response.status_code} from token request to {response.url}")

    if response.status_code == 200:
        if output_option == 'RESPONSE':
            return response
        elif output_option == 'JSON':
            return response.json()
        elif output_option == 'TOKEN':
            return response.json().get('access_token')
        else:
            return f"bearer {response.json().get('access_token')}"

    else:
        logging.error(
            f"access_token request unsuccessfull with status code {response.status_code}")
        return response


def cf_list_resources(resource='apps', output_option='JSON', name=None, auth_header=None, **credentials):
    """List Pivotal Cloud resources with v3 api.
    Documentation: https://v3-apidocs.cloudfoundry.org/version/3.86.0/index.html#resources

    Keyword Arguments:
        resource {str} -- resource type (default: {'apps'})
            suported resources are ``orgs``, ``spaces``,``apps``, ``packages``,``tasks``, ``buildpacks``, ``droplets``.
        output_option {str} -- determines format of output (default: {'JSON'})
            ``RESPONSE`` returns requests.response object
            ``LIST`` returns list of resources from response body
            ``JSON`` returns json body from requests.response object
        name {str} -- name of an instance of resource for filtering (default: {None})
        auth_header {str} -- Bearer access token for authorization headers (default: {None})

        keyword arguments for cf_get_credentials can be passed to override the defaults

    Returns:
        {requests.response, list, dict}
    """

    if not auth_header:
        credentials = cf_get_credentails(**credentials)  
        auth_header = cf_get_auth_header(
            output_option='AUTH_HEADER', **credentials)
    else:
        credentials = cf_get_credentails(**credentials)  

    host = credentials.get('host')

    headers = {"Content-Type": "application/json",
               "Accept": "application/json;charset=UTF-8",
               "Authorization": auth_header}
    if resource == 'orgs':
        url = f"{host}/v3/organizations"
    elif resource == 'spaces':
        url = f"{host}/v3/spaces"
    elif resource == 'tasks':
        url = f"{host}/v3/tasks"
    elif resource == 'buildpacks':
        url = f"{host}/v3/buildpacks"
    elif resource == 'droplets':
        url = f"{host}/v3/droplets"
    elif resource == 'packages':
        url = f"{host}/v3/packages"
    else:
        url = f"{host}/v3/apps"

    response = requests.request('GET', url, verify=False, headers=headers)
    logging.debug(
        f"{response.status_code}response_code for resource {response.url}")

    if output_option == 'RESPONSE':
        return response
    if output_option == 'LIST':
        return response.json().get('resources')
    else:
        if name:
            resources = response.json().get('resources')
#             logging.debug(f"searching for {name} name in thes resources: \n {resources}")
            return [obj for obj in resources if obj.get('name') == name]
        else:
            return response.json()


def cf_get(url, auth_header=None, output_option='JSON', **credentials):
    """Invoke requests.get method on url with v3 API.

    Arguments:
        url {str} -- resource url.

    Keyword Arguments:
        auth_header {str} -- Bearer access token for authorization headers (default: {None})
        output_option {str} -- determines format of output (default: {'JSON'})
            ``RESPONSE`` returns requests.response object
            ``LIST`` returns list of resources from response body
            ``JSON`` returns json body from requests.response object

        keyword arguments for cf_get_credentials can be passed to override the defaults

    Returns:
    {requests.response, list, dict}
    """

    if not auth_header:
        credentials = cf_get_credentails(**credentials)  
        auth_header = cf_get_auth_header(
            output_option='AUTH_HEADER', **credentials)  
    else:
        credentials = cf_get_credentails(**credentials)  

    host = credentials.get('host')
    headers = {"Content-Type": "application/json",
               "Accept": "application/json;charset=UTF-8",
               "Authorization": auth_header}

    response = requests.request('GET', url, verify=False, headers=headers)
#     logging.debug(f"{response.status_code}response_code for resource {response.url}")

    if output_option == 'RESPONSE':
        return response
    if output_option == 'LIST':
        return response.json().get('resources')
    else:
        return response.json()


def cf_requests(method, url, auth_header=None, output_option='JSON', **kwargs):
    """Invoke requests.requests method on url with v3 API.

    Arguments:
        method {str} -- request method for requests.request function
        url {str} -- resource url

    Keyword Arguments:
        auth_header {str} -- Bearer access token for authorization headers (default: {None})
        output_option {str} -- determines format of output (default: {'JSON'})
            ``RESPONSE`` returns requests.response object
            ``LIST`` returns list of resources from response body
            ``JSON`` returns json body from requests.response object

    Returns:
    {requests.response, list, dict}

    """
    if not auth_header:
        credentials = cf_get_credentails()  
        auth_header = cf_get_auth_header(
            output_option='AUTH_HEADER', **credentials)  

    headers = {"Content-Type": "application/json",
               "Accept": "application/json;charset=UTF-8",
               "Authorization": auth_header}

    response = requests.request(
        method, url, verify=False, headers=headers, **kwargs)
#     logging.debug(f"{response.status_code} response_code for resource {response.url}")

    if output_option == 'RESPONSE':
        return response
    if output_option == 'LIST':
        return response.json().get('resources')
    else:
        return response.json()


def cf_download_package(app_name=None, url=None, auth_header=None, output_option='ZIP'):
    """Download package in ZIP, droplet in GZIP format of deployed app with app_name or package_self_link.

    Keyword Arguments:
        app_name {str} -- name of deployed application, if only app_name is provide, latest package will be downloaded (default: {None})
        url {str} -- url of specific package to download (default: {None})
        auth_header {str} -- Bearer access token for authorization headers (default: {None})
        output_option {str} -- format of output (default: {'ZIP'})
            other option include ``io.BytesIO``, ``GZIP``

    Returns:
        {str, io.BytesIO} -- for downloaded ``ZIP`` and ``GZIP`` path as str is returen while IO returns io.BytesIO
    """
    if not auth_header:
        credentials = cf_get_credentails()  
        auth_header = cf_get_auth_header(
            output_option='AUTH_HEADER', **credentials)  

    if url:
        app_name = "my_app"
    else:
        if not app_name:
            logging.error(f"either app_name or package url must be provided")
        else:
            app = cf_list_resources(
                resource='apps', auth_header=auth_header, name=app_name)
            if app:
                app_url = app[0]['links']['self']['href']
            else:
                logging.error(
                    f"{app_name} not found, app name is case sensitive")

        packages_url = f"{app_url}/packages"

        packages_list = cf_get(
            packages_url, auth_header=auth_header, output_option='LIST')
        logging.info(
            f"found {len(packages_list)} packages, downloading guid: {packages_list[-1]['guid']} the created at  {packages_list[-1]['created_at']} ...")

        url = packages_list[-1]['links']['download']['href']

    response = cf_get(url, auth_header=auth_header, output_option='RESPONSE')

    if response.status_code == 200:
        if output_option == 'IO':
            from io import BytesIO
            file_like = BytesIO(response.content)
            file_like.seek(0)

        elif output_option == 'GZIP':
            file_like = f"{url.split('/')[-2]}.gzip"
            with open(file_like, 'wb') as file:
                file.write(response.content)
        else:
            file_like = f"{url.split('/')[-2]}.zip"
            with open(file_like, 'wb') as file:
                file.write(response.content)

        return file_like
    else:
        logging.error("download didn't succeed")
        return response


def cf_create_task(app_name, command, task_name=None, auth_header=None):
    """Create adhoc task.

    Arguments:
        app_name {str} -- name of target app
        command {str} -- command for app

    Keyword Arguments:
        task_name {str} -- descritive name for task (default: {adhoc_task_timestamp in %Y%m%d_%H%M%S format})
        auth_header {str} -- Bearer access token for authorization headers (default: {None})

    Returns:
        {requests.response} -- [description]
    """
    if not auth_header:
        credentials = cf_get_credentails()  
        auth_header = cf_get_auth_header(
            output_option='AUTH_HEADER', **credentials)  

    app = cf_list_resources(
        resource='apps', auth_header=auth_header, name=app_name)

    if app:
        app_url = app[0]['links']['self']['href']
    else:
        logging.error(f"{app_name} not found, app name is case sensitive")

    if not task_name:
        from datetime import datetime
        task_name = f"adhoc_task_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    url = f'{app_url}/tasks'
    data = {"command": command, "name": task_name}
    response = cf_requests('POST', url, auth_header=auth_header,
                           output_option='RESPONSE', data=json.dumps(data))
    logging.info(
        f"{response.status_code} status_code for creating {task_name} task")
    return response
