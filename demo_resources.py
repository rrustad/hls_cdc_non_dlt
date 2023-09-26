import os
import requests
import timeit
import time
import collections
from pyspark.sql.functions import col

def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename
  
def download_file_from_git(dest, owner, repo, path):
    if not os.path.exists(dest):
      os.makedirs(dest)
    from concurrent.futures import ThreadPoolExecutor
    files = requests.get(f'https://api.github.com/repos/{owner}/{repo}/contents{path}').json()
    files = [f['download_url'] for f in files if 'NOTICE' not in f['name']]
    def download_to_dest(url):
         download_file(url, dest)
    with ThreadPoolExecutor(max_workers=10) as executor:
        collections.deque(executor.map(download_to_dest, files))

def table_exists(table_path, spark):
  catalog, schema, table = table_path.split('.')
  tables = spark.table(f'{catalog}.information_schema.tables').filter(col('table_schema') == schema).select('table_name').collect()
  tables = [table['table_name'] for table in tables]
  return table in tables
