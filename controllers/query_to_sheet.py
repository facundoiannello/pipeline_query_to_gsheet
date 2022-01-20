from sources_and_destinations.dbs.pg_connection import PostgresConnectionService
from sources_and_destinations.gsheets.gsheets_base_controller import GsheetsBaseController
from utils import sql_query
import pandas as pd
import logging


logger = logging.getLogger(__name__)


class Query2Gsheet:
    def __init__(self):
        self.db = PostgresConnectionService()
        self.gsheets = GsheetsBaseController()
        self.input_file = 'input_file.json'
    
    def run(self,):
        self.execute_etl()

    def execute_etl(self):
        sheet_url = self.get_sheet_url()
        df = self.prepare_data_to_upload()
        self.upload_result(sheet_url, df)

    def upload_result(self, sheet_url, df):
        logger.info('Uploading results to sheet...') 
        gsheets = self.gsheets
        gsheets.update_spreadsheet(sheet_url, df)

    def prepare_data_to_upload(self):
        logger.info('Preparing data to upload...') 
        query_result = self.get_query_result()
        df = query_result.astype(str)
        return df

    def get_query_result(self):
        logger.info('Querying database...')
        input_file = self.input_file
        query = sql_query.get_sql_query(input_file)['query']
        cursor = self.db.execute_select(query)
        colnames = [desc[0] for desc in cursor.description]
        result = pd.DataFrame(cursor.fetchall(), columns=colnames)
        cursor.close()
        return result

    def get_sheet_url(self):
        input_file = self.input_file
        sheet_url = sql_query.get_sql_query(input_file)['sheet_url']
        return sheet_url


query_2_gsheet = Query2Gsheet()
query_2_gsheet.run()