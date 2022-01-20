import os
from utils.read_files import ReadFiles

resources_dir = os.environ.get('RESOURCES_PATH', 'resources/')

def get_sql_query(file_name):
    file_reader = ReadFiles()
    query_string = file_reader.read_json(resources_dir+file_name)
    return query_string