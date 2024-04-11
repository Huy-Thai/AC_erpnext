import asyncio
import gspread_asyncio

from functools import cache
from dateutil import parser
from google.oauth2.service_account import Credentials


@cache
class GGSheet:
    client_agc = None

    def __init__(self, url_file, worksheet_name):
        self.url_file = url_file
        self.worksheet_name = worksheet_name


    def __credentials(self):
        creds = Credentials.from_service_account_file("../../sheet_service_account.json")
        scoped = creds.with_scopes([
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        return scoped


    async def __authorization(self):
        agcm = gspread_asyncio.AsyncioGspreadClientManager(self.__credentials)
        agc = await agcm.authorize()
        self.client_agc = agc


    async def __open_spreadsheet(self):
        if self.client_agc is None: await self.__authorization()
        sheet = await self.client_agc.open_by_url(self.url_file)
        worksheet = await sheet.worksheet(self.worksheet_name)
        return worksheet


    async def get_values_with_excel_style(self, num_of_row, seed, is_return_num=False):
        new_rows = {}
        worksheet = await self.__open_spreadsheet()
        values = await worksheet.row_values(num_of_row)
        for idx, value in enumerate(values):
            column = excel_style(None, idx + seed)
            new_rows[column] = value

        if is_return_num:
            result = {}
            result[num_of_row] = new_rows
            return result

        return new_rows


    async def update_worksheet(self, num_of_cell, payload):
        if self.client_agc is None: await self.__authorization()
        worksheet = await self.__open_spreadsheet()
        await worksheet.update_acell(f"'A{num_of_cell}'", payload)


    async def get_row_values_by_range(self, row_of_date, range_start, range_end):
        date_values = await self.get_values_with_excel_style(num_of_row=row_of_date, seed=1)
        ignore_values = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"]
        row_date = {k: v for k, v in date_values.items() if k not in ignore_values}

        promises = []
        for num in range(range_start, range_end):
            promise = asyncio.ensure_future(self.get_values_with_excel_style(num_of_row=num, seed=1, is_return_num=True))
            promises.append(promise)
        row_values = await asyncio.gather(*promises)

        return row_values, row_date


def mapping_cell_with_dates_raw(cell, row_date):
    new_date = {}
    date_string = ""
    for column, value in cell.items():
        if column in row_date and value != None and value != "":
            date = parser.parse(row_date[column])
            new_date[date] = value
            date_string = date_string + column + "-" + value + ";"
    return new_date, date_string


def excel_style(row, col):
    """ Convert given row and column number to an Excel-style cell name. """
    LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    while col:
        col, rem = divmod(col-1, 26)
        result[:0] = LETTERS[rem]
    return ''.join(result)
