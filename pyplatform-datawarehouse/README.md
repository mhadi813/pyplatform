### Pyplatform-datawarehouse package provides functions for querying, writing and managing data in Google BiqQuery.

## Installation
```python
pip install pyplatform-datawarehouse
```
## Authentication and environment variables
Refer to [main page](https://github.com/mhadi813/pyplatform) for documentation on authentication

## Exploring the modules
```python

from pyplatform.datawarehouse import *
show_me()
```

## Usage
### querying BigQuery table
```python
import pyplatform.datawarehouse as dw
sql = """SELECT Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment 
FROM `project_id.dataset.sample_superstore` """
df = dw.bq_to_df(sql)
df.head()

# select statemet is enclosed in the stored procedure
sql = """CALL `project_id.dataset.spoc_sample_superstore`();"""
df = dw.bq_to_df(sql)
df.head()

```
### writing dataframe to BigQuery table
```python
import pyplatform.datawarehouse as dw
# df #TODO create dataframe as source
table_id = 'project_id.dataset.sample_superstore'
job = dw.df_to_bq(df,table_id)

print(dw.bq_get_job_info(job))

```