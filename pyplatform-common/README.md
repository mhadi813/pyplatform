### Pyplatform-common package provides utility functions and functions for interacting with APIs and compute services.

## Installation
```python
pip install pyplatform-common
```
## [Authentication](https://github.com/mhadi813/pyplatform)
Refer to main page for documentation on authentication

## Exploring modules
```python

from pyplatform.common import *
show_me()

import pyplatform.datawarehouse as dw
show_me(dw)
```

## Usage
### calling Google catalog-api with core python package
```python

from pyplatform.common.utils import *
import requests

url = 'https://cloudbilling.googleapis.com/v1/services'
headers = {'Authorization' : gcp_get_auth_header(scope='https://www.googleapis.com/auth/cloud-platform')
          ,'Content-Type':'Application/json'}

response  = requests.request('GET',url,headers=headers)
response.status_code

```
### downloading deployed app code package from pivotal cloud
```python

from pyplatform.common.pivotal_cloud import *
cf_download_package(app_name='hello_app')
```