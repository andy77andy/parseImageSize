import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials

scopes = {
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
}

