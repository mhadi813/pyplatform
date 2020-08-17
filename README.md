### Pyplatform is a data analytics platform built around Google BigQuery. This package provides wrapper functions for interacting with cloud services and creating data pipelines using Google Cloud, Microsoft Azure, O365, and Tableau Server as source and destination.

### The platorm architecture:
-  enables fast and scalable SQL datawarehousing service
-  abstracts away the infrastuture by builiding data pipelines with serverless compute solutions in python runtime environments
-  simplifies development environment by using jupyter lab as the main tool

<img align="left" style="width: 1200px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/pyplatform.png">

## Installation
```python
pip install pyplatform
```

## Setting up development environment
```
git clone https://github.com/mhadi813/pyplatform
cd pyplatform
conda env create -f pyplatform_dev.yml
```

## Authentication and environment variables
Credential file path can be set a environment varible in conda env activation script or bash profiles. Please reference [conda documentation](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#saving-environment-variables) for enviroment variables and environment activation script

```python
import os
# if env activation script not created: update path to credential files
# see ``secrets`` folder for credential tamplates
# see functions ``docstring`` for authentication methods when calling a function
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './secrets/dummy_gcp_service_account_credentials.json'
os.environ["AZURE_CREDENTIALS"]= './secrets/dummy_ms_azure_credentials.json' 
os.environ['TABLEAU_SERVER_CREDENTIALS']='./secrets/dummy_tableau_server_credentials.json'
os.environ['PIVOTAL_CREDENTIALS']='./secrets/dummy_pivotal_credentials.json'

os.environ['DATASET'] = 'default_bigquery_dataset_name'
os.environ['STORAGE_BUCKET'] = 'default_storage_bucket_id'
```

## Usage
## common usage patterns:

### - Http sources
<img align="left" style="width: 740px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/http_sources.png">


### - On-prem sources with VPN requirement
<img align="left" style="width: 740px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/on-prem_sources.png">


### - Bigquery integration with Azure Logic Apps
<img align="left" style="width: 740px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/logic_apps_integration.png">


### - Event driven ETL process
<img align="left" style="width: 740px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/event_driven.png">


### - Streaming pipelines
<img align="left" style="width: 740px;" src="https://raw.githubusercontent.com/mhadi813/pyplatform/master/samples/image/streaming.png">


## Exploring the modules
```python

from pyplatform.common import *
show_me()

import pyplatform as pyp
show_me(pyp)
```
