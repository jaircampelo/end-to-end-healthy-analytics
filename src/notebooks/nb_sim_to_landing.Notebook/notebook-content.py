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
# **Notebook:** nb_sim_to_landing.ipynb
# 
# **Description:** This notebook is responsible for the incremental ingestion from Mortality Information System (SIM) to Fabric Lakehouse using PySUS Library, and also creates a table containing the metadata of files downloads, which can be used to monitoring the data ingestion.


# CELL ********************

# Libs
import re
import os
import notebookutils
import pyfabricops as pf
import sempy.fabric as fabric
from pysus import SIM
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
landing_files_path = f'{lakehouse_path}/Files/Landing/SIM'
landing_meta_table_path = f'{lakehouse_path}/Tables/metadata/landing_meta_table' # Track what was copied from SIM to Landing

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

sim = SIM().load()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Project scope
start_year = 2020
end_year = datetime.now().year

# In this project, we only have 2 workspaces: DEV, for the develop branch, and PRD, for the main branch.
# For performance purposes, this evaluation garantees that we only work with a sample data on develop branch.
# And only when we do the pull request for main, the total of data will be showed at the report.
if pf.get_workspace(workspace_id, df=False)['displayName'].endswith('DEV'):
    years = next((y for y in range(end_year, start_year, -1) if sim.get_files('CID10', uf='PA', year=y)), None)
    years = [years, years - 1] if years else []
    ufs = ['PA']
    if not years:
        logger.warning(f'No data avaiable within the specified scope ({start_year}-{end_year}).')
    else:
        logger.success(f'This notebook will extract data from {len(ufs)} state ({ufs}) and {len(years)} years ({years}).')

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
            files = sim.get_files('CID10', uf=uf, year=year)
            if not files:
                files_not_found.append(f'{uf}-{year}')
                continue
            for f in files:
                info = sim.describe(f)
                df_source_files.append({
                    'file_name':            info['name'],
                    'source_size':          info['size'],
                    'source_last_update':   info['last_update'],
                })
        except Exception as e:
            print(f'Error at {uf}-{year}: {e}')

qtt_files_found = len(df_source_files)
qtt_files_not_found = len(files_not_found)

df_source_files = spark.createDataFrame(df_source_files)

logger.info(f'Files not found: {qtt_files_not_found}')
logger.info(f'Files found: {qtt_files_found}')
display(df_source_files.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Mount target path and group name

# CELL ********************

# For each file, creates a path for te respective year and add a column with the name of the group
df_source_files = df_source_files.withColumn(
    'target_path',
    F.concat(
        F.lit(f'{landing_files_path}/'),
        F.regexp_extract(F.col('file_name'), r'(\d{4})', 1),
        F.lit('/'),
        F.substring_index(F.col('file_name'), '.', 1),
        F.lit('.parquet')
    )
).withColumn(
    'organization_name',
    F.lit(sim.metadata['long_name'])
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
    F.col('organization_name'),
    F.col('file_name'),
    F.round(convert_to_mb('source_size'), 2).alias('source_size_mb'),
    F.to_timestamp(F.col('source_last_update'), 'yyyy-MM-dd hh:mma').alias('source_last_update'),
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
    StructField('organization_name',    StringType(),       False),
    StructField('file_name',            StringType(),       False),
    StructField('source_size_mb',       DoubleType(),       False),
    StructField('source_last_update',   TimestampType(),    False),
    StructField('target_path',          StringType(),       True),
    StructField('copied_at',            TimestampType(),    True),
])

spark.createDataFrame([], landing_meta_schema) \
    .write.format('delta') \
    .mode('ignore') \
    .save(landing_meta_table_path)

df_landing_meta = spark.read.format('delta').load(landing_meta_table_path)

# Give the last copied info per file_name
df_landing_meta_latest = df_landing_meta.groupBy('file_name').agg(
        F.max('source_last_update').alias('last_copied_source_mtime')
    )

# Find candidates to copy
df_candidates = (
    df_source_files
    .join(df_landing_meta_latest, on='file_name', how='left')
    .filter(F.col('source_last_update') > F.coalesce(F.col('last_copied_source_mtime'), F.lit('1970-01-01')))
    .select(
        'organization_name',
        'file_name',
        'source_last_update',
        'source_size_mb',
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
        'organization_name',
        'file_name',
        'source_last_update',
        'source_size_mb',
        'target_path',
    ).collect()

    # Download files and prepare for update landing_meta_table
    downloaded_files = []
    for row in candidates:
        organization_name   = row['organization_name']
        file_name           = row['file_name']
        source_last_update  = row['source_last_update']
        source_size_mb      = row['source_size_mb']
        target_path         = row['target_path']
        year                = re.search(r'\d{4}', row['file_name']).group()
        uf                  = re.search(r'([A-Za-z]{2})(?=\d{4})', row['file_name']).group()

        try:
            # Download files
            logger.info(f'Trying to downloading {file_name}')
            files = sim.get_files('CID10', uf=uf, year=year)
            file_obj = next((f for f in files if f.basename == file_name), None)
            if file_obj is None:
                logger.error(f'File not found: {file_name}')
            else:
                logger.success(f'Successfully downloaded {file_name}.')  
            df_pandas = file_obj.download().to_dataframe()
            df = spark.createDataFrame(df_pandas)
            df.write.mode('overwrite').parquet(target_path)

            # Record Metadata
            downloaded_files.append({
                'organization_name':    organization_name,
                'file_name':            file_name,
                'source_last_update':   source_last_update,
                'source_size_mb':       source_size_mb,
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

# MARKDOWN ********************

# ## Don't forget to clean all outputs at the end.
