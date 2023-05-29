import gspread
import pandas as pd
import logging

import pytz
import tkinter as tk

from googleapiclient.errors import HttpError
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from datetime import datetime

from tkinter import ttk
from tkinter import filedialog

from _secret import DAILY_PENDING

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
log_format = '%(asctime)s | %(levelname)s: %(message)s'
console_handler.setFormatter(logging.Formatter(log_format))
logger.addHandler(console_handler)

SCOPE = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

CREDS = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', SCOPE)
CLIENT = gspread.authorize(CREDS)

try:
    SERVICE = build('sheets', 'v4', credentials=CREDS)
except:
    DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
    SERVICE = build('sheets', 'v4', credentials=CREDS, discoveryServiceUrl=DISCOVERY_SERVICE_URL)

DATA = {

    'folder_id': {
        'daily_pending': DAILY_PENDING

    },
    'range_name': 'A1:ZZ1000000',

}


def text_split(file_name):
    with open(file_name, 'r') as f:
        data = f.read()

    rows = data.split('\n')

    return [row.split() for row in rows]


def insert_table(file_name, spreadsheet_name, folder_id=None):
    spreadsheet_id = create_spreadsheet(spreadsheet_name, folder_id)

    client = gspread.authorize(CREDS)

    sheet = client.open(spreadsheet_name).sheet1

    with open(file_name, 'r', encoding="Windows_1251") as f:
        content = f.read()

    body = {'values': content}

    result = SERVICE.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=DATA['range_name'],
        valueInputOption='USER_ENTERED', body=body).execute()(timeout=60)

    logger.info('{0} cells updated.'.format(result.get('updatedCells')))


def hide_columns(spreadsheet_id, column_indexes):
    try:
        SERVICE = build('sheets', 'v4', credentials=CREDS)
    except:
        DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        SERVICE = build('sheets', 'v4', credentials=CREDS, discoveryServiceUrl=DISCOVERY_SERVICE_URL)

    # get the list of sheets in the spreadsheet
    sheet_metadata = SERVICE.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')

    # iterate through each sheet and hide the specified column
    for sheet in sheets:
        sheet_id = sheet['properties']['sheetId']
        requests = []
        for column_index in column_indexes:
            request = {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": column_index,
                        "endIndex": column_index + 1
                    },
                    "properties": {
                        "hiddenByUser": True
                    },
                    "fields": "hiddenByUser"
                }
            }
            requests.append(request)
        try:
            SERVICE.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()
            logger.info(f"Columns hidden on sheet '{sheet['properties']['title']}'")
        except HttpError as error:
            logger.error(f"An error occurred: {error}")


def create_spreadsheet(spreadsheet_name, folder_id=None):
    # Authenticate and create the Sheets API client

    try:
        drive_service = build('drive', 'v3', credentials=CREDS)
    except:
        discoveryUrl = ('https://www.googleapis.com/discovery/v1/apis/drive/v3/rest')
        drive_service = build('drive', 'v3', credentials=CREDS, discoveryServiceUrl=discoveryUrl)

    # Create the spreadsheet metadata dictionary
    spreadsheet_metadata = {
        'name': spreadsheet_name,
        'mimeType': 'application/vnd.google-apps.spreadsheet'
    }

    # If a folder ID is specified, add it to the parents list in the metadata dictionary
    if folder_id is not None:
        spreadsheet_metadata['parents'] = [folder_id]

    # Create the spreadsheet with the specified metadata and parents
    spreadsheet = None
    try:
        spreadsheet = drive_service.files().create(body=spreadsheet_metadata).execute()
    except HttpError as error:
        print(f'An error occurred while creating the spreadsheet: {error}')
        return None

    logger.info(f'New daily pending spreadsheet created | spreadsheet id : {spreadsheet["id"]}')

    return spreadsheet['id']


def main_filter(file_name):
    df = pd.read_csv(file_name, sep='\t', header=0, encoding="Windows-1251", index_col=False, low_memory=False)

    # Delete today's date
    today = datetime.now().strftime('%m/%d/%Y')
    # today = '04/24/2023'
    rows_to_delete = df.loc[df['Collection Date'] == today]
    df = df.drop(rows_to_delete.index)

    # Delete all records that don't have "not verified" status
    rows_to_keep = df.loc[df['Date/Time Verified'] == "NOT VERIFIED"]
    df = rows_to_keep.dropna(how='all')

    # Delete all tests

    prefixes_to_remove = ['LABQ', 'TEST', 'ALINITY', 'CAP', 'REPEAT', 'REPAET', 'PATIENT', 'CBC']

    next_rows_to_keep = df.loc[
        ~df['Patient Last Name'].str.startswith(tuple(prefixes_to_remove)) &
        ~df['Patient First Name'].str.startswith(tuple(prefixes_to_remove)) &
        ~df['Patient Last Name'].str.isnumeric() &
        ~df['Patient First Name'].str.isnumeric()
        ]

    df = next_rows_to_keep.dropna(how='all')

    # sort the dataframe by multiple columns

    df['Test Code'] = df['Test Code'].str.strip()

    df = df.sort_values(by=['Test Code'])

    logger.info("Success | Main Filter | Removed unnecessary data | Sorted")

    return df


def core_lab_filter(df):
    core_lab = df.copy()

    def is_valid_code(code):
        code = code.replace(" ", "")
        if code in ['P024', 'P025', 'CB24']:
            return False
        elif code == 'WBC':
            return True
        else:
            try:
                code_int = int(code)
                return code_int >= 0 and code_int <= 1900
            except ValueError:
                return code.isalpha() or (code[0] not in ['W', 'V'] and code.isalnum())

    # filter the dataframe
    mask = core_lab['Test Code'].apply(is_valid_code)
    core_lab_data = core_lab.loc[mask]

    return core_lab_data


def send_outs_filter(df):
    send_out = df.copy()

    send_out = send_out[send_out['Test Code'].str.startswith('V')]

    return send_out


def quantiferon_filter(df):
    quantiferon = df.copy()

    quantiferon = quantiferon[quantiferon['Test Code'].str.match(r'W\d{3}')]

    return quantiferon


def covid_filter(df):
    covid = df.copy()

    covid = covid[covid["Test Code"].isin(["2024", "2023", "8024"])]

    return covid


def flu_filter(df):
    flu = df.copy()

    flu = flu[flu["Test Code"].isin(["P024", "P025", "2293", "2294", "2295"])]

    return flu


def all_2305_filter(df):
    all_2305 = df.copy()

    all_2305 = all_2305[all_2305["Test Code"].isin(["2305"])]

    return all_2305


def not_verified_filter(df):
    not_verified = df.copy()

    def first_filter(code):
        code = code.replace(" ", "")

        if code in ['2024', '2023', '8024', 'P024', 'P025', '2293', '2294', '2295']:
            return True
        else:
            return False

    mask_1 = not_verified['Test Code'].apply(first_filter)
    not_verified = not_verified.loc[mask_1]

    def second_filter(code):
        code = code.replace(" ", "")

        if code in ["INVALID", 'PRESMPOS', 'PREMPOS', 'NOT VERIFIED']:
            return True
        else:
            return False

    mask_2 = not_verified['Result'].apply(second_filter)
    not_verified = not_verified.loc[mask_2]

    return not_verified


def california_filter(df):
    california = df.copy()

    california = california[california["Test Code"].isin(["CB24"])]

    return california


def sort_to_sheets(df, spreadsheet_title):
    insert_dataframe(core_lab_filter(df), 'CORE LAB', spreadsheet_title)
    insert_dataframe(send_outs_filter(df), 'SEND OUTS', spreadsheet_title)
    insert_dataframe(quantiferon_filter(df), 'QUANTIFERON', spreadsheet_title)
    insert_dataframe(covid_filter(df), 'COVID', spreadsheet_title)
    insert_dataframe(flu_filter(df), 'FLU', spreadsheet_title)
    insert_dataframe(all_2305_filter(df), 'ALL 2305', spreadsheet_title)
    insert_dataframe(not_verified_filter(df), 'NOT VERIFIED', spreadsheet_title)
    insert_dataframe(california_filter(df), 'CALIFORNIA', spreadsheet_title)


def insert_dataframe(df, worksheet_title, spreadsheet_title):
    client = gspread.authorize(CREDS)
    sheet = client.open(spreadsheet_title).worksheet(worksheet_title)
    set_with_dataframe(sheet, df)

    logger.info(f'Successfully inserted data | {worksheet_title}')


def create_worksheets(spreadsheet_title):
    client = gspread.authorize(CREDS)
    spreadsheet = client.open(spreadsheet_title)

    spreadsheet.add_worksheet(title='CORE LAB', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='SEND OUTS', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='QUANTIFERON', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='COVID', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='FLU', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='ALL 2305', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='NOT VERIFIED', rows=1000, cols=50)
    spreadsheet.add_worksheet(title='CALIFORNIA', rows=1000, cols=50)

    logger.info('Successfully created all worksheets | 8 sheets')


def generate_name_for_spreadsheet():
    new_york_tz = pytz.timezone("America/New_York")
    date_in_new_york = datetime.now(new_york_tz).strftime("%m/%d/%Y")

    logger.info(f"Successfully generated spreadsheet name | Daily Pending {date_in_new_york}")

    return f"Daily Pending {date_in_new_york}"


def create_range():
    ranges_to_hide = [
        (2, 5),  # Range B-E
        (11, 16),  # Range K-P
        (18, 32),  # Range R-AG
        (36, 63),  # Range AJ-BK
        (70, 101),  # Range BR-CW
        (104, 110)  # Range CZ-DF
    ]

    # compute the column indexes for each range
    column_indexes = []
    for start, end in ranges_to_hide:
        column_indexes += list(range(start - 1, end))

    return column_indexes


def delete_worksheet(spreadsheet_id, sheet_id=0):
    try:
        SERVICE = build('sheets', 'v4', credentials=CREDS)
    except:
        DISCOVERY_SERVICE_URL = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
        SERVICE = build('sheets', 'v4', credentials=CREDS, discoveryServiceUrl=DISCOVERY_SERVICE_URL)

    # create the request to delete the worksheet
    request = {
        'deleteSheet': {
            'sheetId': sheet_id
        }
    }

    # send the request to the API
    try:
        response = SERVICE.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={'requests': [request]}
        ).execute()
        logger.info(f"Worksheet with ID {sheet_id} deleted successfully.")
    except HttpError as error:
        logger.error(f"An error occurred: {error}")


def select_file():
    file_path = filedialog.askopenfilename()
    # do something with the file_path, such as process it
    main(file_path)

    # simulate processing by iterating over a range
    for i in range(100):
        # update the progress bar
        progress_var.set(i)
        progress_bar.update()

    # update the label to show that the process is finished
    status_label.config(text="Processing finished!")

    
def main(file_name):
    spreadsheet_title = generate_name_for_spreadsheet()
    folder_id = DATA['folder_id']['daily_pending']

    spreadheet_id = create_spreadsheet(spreadsheet_title, folder_id)
    create_worksheets(spreadsheet_title)

    sort_to_sheets(main_filter(file_name), spreadsheet_title)
    delete_worksheet(spreadheet_id, sheet_id=0)
    hide_columns(spreadheet_id, create_range())


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Daily Pending Reports")
    root.geometry('500x200')

    # create a button to select a file
    select_button = tk.Button(root, text="Select File", command=select_file)
    select_button.pack(pady=10)

    # create a progress bar to show the progress of the process
    progress_var = tk.DoubleVar()
    progress_bar = ttk.Progressbar(root, variable=progress_var, maximum=100)
    progress_bar.pack(pady=10)

    # create a label to show the status of the process
    status_label = tk.Label(root, text="")
    status_label.pack(pady=10)

    root.mainloop()
