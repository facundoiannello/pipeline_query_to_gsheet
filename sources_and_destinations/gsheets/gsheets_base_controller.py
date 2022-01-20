import os
import gspread
import pandas as pd

resources_dir = os.environ.get('RESOURCES_PATH', '')
gsheets_creds_file = 'gsheets_credentials.json'

class GsheetsBaseController:
    def __init__(self):
        self.resources=resources_dir
        self.credentials_filename=gsheets_creds_file
        self.authorized_user_filename=gsheets_creds_file
    
    def update_spreadsheet(self, sheet_url, df):
        credentials_filename = self.credentials_filename
        authorized_user_filename = self.authorized_user_filename
        gc = self.authenticate(credentials_filename, authorized_user_filename)
        sh = gc.open_by_url(sheet_url).sheet1
        sh.update([df.columns.values.tolist()] + df.values.tolist())

    def authenticate(self, creds, authorized_user):
        credentials_file = self.resources + creds
        authorized_user_file = self.resources + authorized_user
        gc = gspread.oauth(
            credentials_filename=credentials_file
        )
        return gc



