### Pyplatform-database package provides read and write functions for Microsoft SQL database.

## Installation
```python
pip install pyplatform-database
```

## Authentication and environment variables
Refer to [main page](https://github.com/mhadi813/pyplatform) for documentation on authentication

## Exploring the modules
```python

from pyplatform.database import *
show_me()
```

## Usage
### reading from SQL table, default database provided in credential file
```python
from pyplatform.database import *
query = """SELECT * FROM dbo.fact_sample_superstore"""
df = az_to_df(query)
df.head()

```

### writing pandas dataframe to sql table with non default setting
```python

from pyplatform.database import *
# df #TODO create dataframe as source
table_name = 'dbo.fact_sample_superstore'
engine = azure_sql_engine(database='testDB')  #changing database only
df_to_azure_sql(df,table_name, engine=engine)

```