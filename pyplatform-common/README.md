### Pyplatform-common package provides utility, file management and authentication functions for interacting with APIs and compute services.

## Installation
```python
pip install pyplatform-common
```

## Authentication and environment variables
Refer to [main page](https://github.com/mhadi813/pyplatform) for documentation on authentication

## Exploring the modules
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

### downloading deployed application code package from pivotal cloud
```python

from pyplatform.common.pivotal_cloud import *
cf_download_package(app_name='hello_app')
```