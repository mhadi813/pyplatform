### Pyplatform-datalake package provides functions for Google Cloud Storage and Microsoft Storage services.

## Installation
```python
pip install pyplatform-datalake
```

## Authentication and environment variables
Refer to [main page](https://github.com/mhadi813/pyplatform) for documentation on authentication

## Exploring the modules
```python

from pyplatform.datalake import *
show_me()
```

## Usage
### uploading in-memory data to Google Cloud Storage
```python
import pyplatform.datalake as dlk
from io import BytesIO
# df #TODO create dataframe as source
in_mem_file = BytesIO()
df.to_excel(in_mem_file,index=False)
in_mem_file.seek(0)
dlk.gcs_upload_blob(in_mem_file,bucket_id='bucket',blobname='test.xlsx')

```

### downloading and loading trained forecasting model from Google Cloud Storage
```python
import pyplatform.datalake as dlk
from io import BytesIO
import pickle

gcs_uri = 'gs://bucket/folder_name/model_name'
model_name="fbprophet_forecast_daily"
dlk.gcs_download_blob(gcs_uri,filepath=model_name)
with open(model_name,"rb") as model:
    my_model=pickle.load(model)

```