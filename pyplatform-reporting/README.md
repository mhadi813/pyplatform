### Pyplatform-reporting package provides function for working functions for working with Tableau server and hyper datasources.

## Installation
```python
pip install pyplatform-reporting
```
## [Authentication](https://github.com/mhadi813/pyplatform)
Refer to main page for documentation on authentication

## Exploring modules
```python

from pyplatform.reporting import *
show_me()
```

## Usage
### listing Tableau server datasources and reading hyper datasource into pandas dataframe
```python
from pyplatform.reporting import *

tableau_server_list_resources(resource='datasources',output_option='DICT')
datasource_name = 'sample superstore'
hyper_filepath = tableau_server_download_hyper(datasource_name)
df= hyper_to_df(hyper_filepath)

```
### uploading pandas dataframe as hyper datasource on Tableau server
```python
from pyplatform.reporting import *
# df #TODO create dataframe as source
hyper_filepath = df_to_hyper(df,filepath='Sample superstore.hyper')
datasource_name = tableau_server_upload_hyper(hyper_filepath)

```