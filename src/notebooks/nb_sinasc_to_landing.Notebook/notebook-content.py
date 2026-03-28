# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "2ac0a72e-b750-9aed-4866-fa2a1c3a97fe",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Incremental data ingestion from DATASUS to Microsoft Fabric using PySUS Library, PySpark Notebooks and Data Pipeline
# 
# ㅤ
# 
# >ㅤ\
# > This project uses the **DATASUS database**, which promotes modernization through information technology to support Brazil's **Unified Health System (SUS)**.\
# >ㅤ\
# > The dataset consists of .parquet files, downloaded using PySUS Library, which is a package of a set of utilities for handling with public databases published by Brazil's DATASUS.\
# >ㅤ\
# > The documentation of how to use PySUS can be found at [https://github.com/AlertaDengue/PySUS](https://github.com/AlertaDengue/PySUS) and [https://pysus.readthedocs.io/en/latest/](https://pysus.readthedocs.io/en/latest/)\
# >ㅤ
# 
# ㅤ
# 
# **Notebook:** nb_sinasc_to_landing.ipynb
# 
# **Description:** This notebook is responsible for the incremental ingestion from Brazil Live Birth Information System (SINASC) to Fabric Lakehouse using PySUS Library, and also creates a table containing the metadata of files downloads, which can be used to monitoring the data ingestion.


# CELL ********************

# Libs
import os
import notebookutils
import pyfabricops as pf
import sempy.fabric as fabric
from pysus import SINASC
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Authentication and logging

# CELL ********************

notebookutils.credentials.getToken('keyvault')
authentication_method = 'fabric'
key_vault = 'https://kv-jaircampelo.vault.azure.net/'
pf.set_auth_provider(authentication_method)

pf.setup_logging('info', 'detailed', include_colors=True)
logger = pf.get_logger(__name__)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters

# CELL ********************

# Fabric scope
workspace_id = fabric.get_notebook_workspace_id()
lakehouse_name = 'lh_healthy_analytics'
lakehouses = notebookutils.lakehouse.list()
lakehouse = next((item for item in lakehouses if item.get('displayName') == lakehouse_name), '')
lakehouse_path = lakehouse['properties']['abfsPath']
landing_files_path = f'{lakehouse_path}/Files/Landing/SINASC'
landing_meta_table_path = f'{lakehouse_path}/Tables/dbo/SinascLandingMeta' # Track what was copied from SIM to Landing

print(f'Files path: {landing_files_path}')
print(f'Metatable path: {landing_meta_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load data from DATASUS

# CELL ********************

sinasc = SINASC().load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Project scope
start_year = 2018
end_year = datetime.now().year

# In this project, we only have 2 workspaces: DEV, for the develop branch, and PRD, for the main branch.
# For performance purposes, this evaluation garantees that we only work with a sample data on develop branch.
# And only when we do the pull request for main, the total of data will be showed at the report.
if pf.get_workspace(workspace_id, df=False)['displayName'].endswith('DEV'):
    years = next((y for y in range(end_year, start_year, -1) if sinasc.get_files('DN', uf='PA', year=y)), None)
    years = [years, years - 1] if years else []
    ufs = ['PA']
    if not years:
        logger.warning(f'No data avaiable within the specified scope ({start_year}-{end_year}).')
    else:
        logger.success(f'This notebook will extract data from {len(ufs)} state and {len(years)} years.')

else:
    years = list(range(end_year, start_year - 1, -1))
    ufs = [
        'AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO', 'AL', 'BA',
        'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE', 'DF', 'GO',
        'MT', 'MS', 'ES', 'MG', 'RJ', 'SP', 'PR', 'RS', 'SC'
        ]
    if not years:
        logger.warning(f'No data avaiable within the specified scope ({start_year}-{end_year}).')
    else:
        logger.success(f'This notebook will extract data from {len(ufs)} states and {len(years)} years.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## List files from PySUS

# CELL ********************

df_source_files = []
files_not_found = []

for uf in ufs:
    for year in years:
        try:
            files = sinasc.get_files('DN', uf=uf, year=year)
            if not files:
                files_not_found.append(f'{uf}-{year}')
                continue
            for f in files:
                info = sinasc.describe(f)
                df_source_files.append({
                    'file_name':            info['name'],
                    'source_size':          info['size'],
                    'source_last_update':   info['last_update'],
                    'year':                 info['year'],
                })
        except Exception as e:
            print(f'Error at {uf}-{year}: {e}')

qtt_files_found = len(df_source_files)
qtt_files_not_found = len(files_not_found)

df_source_files = spark.createDataFrame(df_source_files)
df_source_files = df_source_files.withColumn(
        'uf',
        F.substring(F.col('file_name'), -10, 2).alias('uf')
)

logger.info(f'Files not found: {qtt_files_not_found}')
logger.info(f'Files found: {qtt_files_found}')
display(df_source_files.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Mount target path

# CELL ********************

# For each file, creates a path for te respective year
df_source_files = df_source_files.withColumn(
    'target_path',
    F.concat(
        F.lit(f'{landing_files_path}/'),
        F.col('year'),
        F.lit('/'),
        F.substring(F.col('file_name'),1, 8),
        F.lit('.parquet')
    )
)

# Define function to transform source_size column to int
def convert_to_mb(col_name):
    '''
    Cast a file size column into an integer type and standadize the format to 'mb'
    '''
    number = F.regexp_extract(F.col(col_name), r'([\d.]+)', 1).cast('double')
    unit = F.regexp_extract(F.col(col_name), r'([a-zA-Z]+)', 1)
    return (
        F.when(unit == 'B', number / (1024 * 1024))
        .when(unit == 'kB', number / 1024)
        .when(unit == 'MB', number)
        .when(unit == 'GB', number * 1024)
        .otherwise(None)
    ).cast('double')

df_source_files = df_source_files.select(
    F.col('file_name'),
    F.round(convert_to_mb('source_size'), 2).alias('source_size_mb'),
    F.to_timestamp(F.col('source_last_update'), 'yyyy-MM-dd hh:mma').alias('source_last_update'),
    F.col('uf'),
    F.col('year'),
    F.col('target_path'),
)

display(df_source_files)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Find candidates to copy

# CELL ********************

# Create landing_meta_table
landing_meta_schema = StructType([
    StructField('file_name',            StringType(),       False),
    StructField('source_size_mb',       DoubleType(),       False),
    StructField('source_last_update',   TimestampType(),    False),
    StructField('year',                 IntegerType(),      False),
    StructField('uf',                   StringType(),       False),
    StructField('target_path',          StringType(),       True),
    StructField('copied_at',            TimestampType(),    True),
])

spark.createDataFrame([], landing_meta_schema) \
    .write.format('delta') \
    .mode('ignore') \
    .save(landing_meta_table_path)

df_landing_meta = spark.read.format('delta').load(landing_meta_table_path)

# Give the last copied info per file_name
df_landing_meta_latest = df_landing_meta.groupBy('uf').agg(
        F.max('year').alias('last_copied_source_year')
    )

# Find candidates to copy
df_candidates = (
    df_source_files
    .join(df_landing_meta_latest, on='uf', how='left')
    .filter(F.col('year') > F.coalesce(F.col('last_copied_source_year'), F.lit(1970)))
    .select(
        'file_name',
        'source_last_update',
        'source_size_mb',
        'year',
        'uf',
        'target_path',
    )
)

logger.success(f'Files to download now (new or updated): {df_candidates.count()}')
display(df_candidates.orderBy('file_name'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Copy files from DATASUS to Landing Zone

# CELL ********************

if df_candidates.isEmpty():
    print('Nothing to download. Skipping...')
else:
    # Candidates from DataFrame to List to iterate
    candidates = df_candidates.select(
        'file_name',
        'source_last_update',
        'source_size_mb',
        'year',
        'uf',
        'target_path',
    ).collect()

    # Download files and prepare for update landing_meta_table
    downloaded_files = []
    for row in candidates:
        file_name = row['file_name']
        source_last_update = row['source_last_update']
        source_size_mb = row['source_size_mb']
        year = row['year']
        uf = row['uf']
        target_path = row['target_path']

        try:
            # Download files
            files = sinasc.get_files('DN', uf=uf, year=year)
            file_obj = next((f for f in files if f.basename == file_name), None)
            if file_obj is None:
                print(f'File not found: {file_name}')
                continue
            df_pandas = file_obj.download().to_dataframe()
            df = spark.createDataFrame(df_pandas)
            df.write.mode('overwrite').parquet(target_path)

            # Record Metadata
            downloaded_files.append({
                'file_name':            file_name,
                'source_last_update':   source_last_update,
                'source_size_mb':       source_size_mb,
                'year':                 year,
                'uf':                   uf,
                'target_path':          target_path,
                'copied_at':            datetime.now()
            })
        except Exception as e:
            print(f'Error downloading {file_name} to {target_path}: {e}')
    
    # Create DataFrame with copied files
    df_copied = spark.createDataFrame(
        downloaded_files,
        schema=spark.read.format('delta').load(landing_meta_table_path).schema
    )

    # Write metadata into the metatable
    df_copied.write.format('delta').mode('append').save(landing_meta_table_path)

    logger.success(f'Copied files to Lakehouse successfully: {len(downloaded_files)}')
    display(df_copied.orderBy('file_name'))
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
