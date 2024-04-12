import os
import asyncio
import gspread_asyncio

from functools import cache
from dateutil import parser
from google.oauth2 import service_account

@cache
class GGSheet:
    client_agc = None

    def __init__(self, url_file, worksheet_name):
        self.url_file = url_file
        self.worksheet_name = worksheet_name
        

    def __credentials(self):
        creds = service_account.Credentials.from_service_account_info({
            "type": "service_account",
            "project_id": "api-project-131277161203",
            "private_key_id": "59f622186740efc7087424ddb825c6923fc96324",
            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCpfwkw89HlSZ+h\naIDdRG7PQ7NYgVIK2hstb/Sn2QHa841Zb+UZmBR34sK3H/L2kkPKfB1ZwFD7gzeX\n4G3iqq6JnOmOskJ6C1LMV+D3X30UvsaqVJQMbO4PebWXsV10YB4PSSJUUV0pg15A\nfKcJwR/hyUXuv8PW/RKBiEKKBxNYA7iYbD3RoJi4iBBBv/ih/Irf/ggD+6qh3TrT\nzQFbR5OAa1LOPdqm5ax+zctTuArhh+K0aJveEmI2uksHpVyEO20ost3LNo0C5CfL\nhzf0E5otNJMIhzWPt8Nps/lYw6NFkDrn6DAW/ALvr82BuhxIjMUsW5klDp2b50ao\niNzhY78nAgMBAAECgf9JJQMz7BzAva+TZedCuhS2OIEUjA+et6VkV9/narKG1pPg\nBMs60pcnPcCY5M8xydScBwwRNWcSkMrCRhfQS7+Yx+2IszP85LVQbY/kz2jJixDD\nAvnUrAjrZ8tXYfZH4OmqQVMJKHUkH72xa+jy91iBtfioregGfZF+5wVucvSQvRlv\nBeAXj5uyhFoqkKsXiPtmiLrEa/bZwyxCtbnGLdsn8IWvgGUeXXhFAM+8f4UdewM6\nBAub4ZiUsSq4jn7iqeYBy9AFZsCw+OF3V35+Y6giAheBj5jHsZRAGxS/H7JNC4Na\nyyyZbW99VG1DTPvS9+xyltXlh1qVympswkP2mIECgYEA2crxPXzf+D6ByFBmlmM7\nUf6GftnOLYtI/AKLnEp59/pxgGsGg3Z9Ff0qtq8OLnzqHut5PCYOKsJegvupsYnU\nUl0YhWlw3XwySd1Ge9ttQFE2enEFpQFK4nYV5TNVR/aWKRKoude9y1RB7fHdVZ5d\nSIZVxYaG70w/6JMEIH0kv9sCgYEAxzsZu8LbzeRT2Va6GRvbF5hvux1bruJ3uM43\nft5D3wzZdsmPpoZCz4S78PGEviwCGmwRuRXCTqjgjyUezrLcde5L+iEqg2Tnmny+\ngm+mqhV8eNWpmiN9ex9h1bO+Gja/k44jF27XhAwxmRzaN+ue25L1ZCYSd1D5Wg24\niwyfdaUCgYAqdDPYP4pNEqoryPhmYkuC7TF8cqqNGDSO41QhkCb8XrZXSQWJBMTX\nT3VPDQqfpzvf8Ri9z9E9JoxTzgjDEdHwiDMqdmZI1lfbLCX8KMbAHdSXw4ZNJtZZ\nFJmqBvqdv4R/1yJKr7JQe0kqv9XcRbV7WKxJh7Kv3NYsWNQaHSrXtwKBgQCefG8+\nJAaCxQ3GqO0lqDkjjgnjybjzaAhhJPqUm+9V8nzTuAfkKo3fUvHG+/ni5lNN/YYj\nvCF0PXdVp+vX9gTWc5hRBC0zlQOAq5dJX9QvHSSFY0Kl8XGSjiZfv7qMU90WXk2g\nCHa8/o4+BOu67F7UwRUgdADglbOmZZ/WTVZUWQKBgQCqIOWoOUCevvE9IY1PZAUF\nEkua4aLxn+KPT5tTZ63xZa9ELc9Txd/07ZP3B3LGH+RfhEmObf2iG10ZKzQIKplK\nvMyTTH+NaX3+oUo2bQ173G9bUW6Rfcxp6T/HtTvVk5rsVKY9U1LQDOhqrsP8OzAp\nwmqvIEforHmxl2cJ6b9p8g==\n-----END PRIVATE KEY-----\n",
            "client_email": "ac-timesheets@api-project-131277161203.iam.gserviceaccount.com",
            "client_id": "116123596473197647748",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ac-timesheets%40api-project-131277161203.iam.gserviceaccount.com",
            "universe_domain": "googleapis.com"
        })
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
        for ignore in ignore_values:
            date_values.pop(ignore, None)

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
