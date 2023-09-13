import asyncio
import hashlib

from datetime import datetime
from frappe.desk.form.assign_to import add as add_assignment
from aiohttp import ClientSession
from functools import cache

_TENANT_ID = "acfde157-8636-4952-b4e3-ed8fd8e274e9"
_CLIENT_ID = "c9eb157c-a854-4438-aca2-0a72b6866c8f"
_CLIENT_SECRET = "T4E8Q~7fpSTGKCoTxeg0_ss11LJYOaQ-McwRobAi"

TASK_REQUIRED_COLUMN = ["B","C","E","F","L","M","N","O","P"]
TASK_PRIORITY = { "": "Medium", "1_Urgen": "Urgent", "2_Important": "High", "3_Medium": "Medium", "7_Transfer": "Medium" }
TASK_STATUS = { "": "Open", "10%": "Working", "20%": "Working", "30%": "Working", "50%": "Working", "70%": "Working", "80%": "Working", "100%": "Completed" }
TIME_SHEET_STATUS = { "": "Draft", "Open": "Draft", "Working": "Draft", "Completed": "Completed", "Cancelled": "Cancelled" }
TIME_SHEET_STATUS_CANCEL_UPDATE = ["Completed", "Cancelled", "Submitted"]

@cache
class MSGraph:
    access_token = None

    def __init__(self, session, site_name=None, folder_name=None, file_name=None, worksheet_name=None):
        self.session = session
        self.site_name = site_name
        self.folder_name = folder_name
        self.file_name = file_name
        self.worksheet_name = worksheet_name


    # TODO: Check expired access token with 1 hour
    async def get_access_token(self):
        AUTH_URL = f"https://login.microsoftonline.com/{_TENANT_ID}/oauth2/v2.0/token"
        PAYLOAD = {
            "grant_type": "client_credentials",
            "client_id": _CLIENT_ID,
            "scope": "https://graph.microsoft.com/.default",
            "client_secret": _CLIENT_SECRET,
        }

        resp = await http_client(url=AUTH_URL, session=self.session, payload=PAYLOAD)
        self.access_token = resp["access_token"] if resp else None
        return


    async def get_site(self):
        assert self.site_name != None, "Param site_name is required"
        SITES_URL = "https://graph.microsoft.com/v1.0/sites"
        resp = await http_client(url=SITES_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.site_name)
        return result


    async def get_folder(self, site_id):
        assert self.folder_name != None and site_id != None, "Param folder_name and site_id are required"
        FOLDERS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        resp = await http_client(url=FOLDERS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.folder_name)
        return result


    async def get_items_in_folder(self, site_id, folder_id):
        assert self.file_name != None and site_id != None and folder_id != None, "Param file_name and site_id and folder_id are required"
        ITEMS_FOLDER_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{folder_id}/children"
        resp = await http_client(url=ITEMS_FOLDER_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.file_name)
        return result


    async def get_worksheet(self, site_id, file_id):
        assert self.worksheet_name != None and file_id != None and site_id != None, "Param worksheet_name and file_id and site_id are required"
        WORKSHEETS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets"
        resp = await http_client(url=WORKSHEETS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.worksheet_name)
        return result


    async def get_worksheet_detail(self, site_id, file_id, worksheet_id, range_rows):
        assert worksheet_id != None and file_id != None and site_id != None, "Param worksheet_id and file_id and site_id are required"
        WORKSHEET_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets/{worksheet_id}"
        WORKSHEET_DETAIL_URL = WORKSHEET_URL + f"/range(address='{range_rows}')?$select=text"
        result = await http_client(url=WORKSHEET_DETAIL_URL, session=self.session, access_token=self.access_token)
        return result


    async def patch_worksheet(self, site_id, file_id, worksheet_id, range_rows, payload):
        assert worksheet_id != None and file_id != None and site_id != None, "Param worksheet_id and file_id and site_id are required"
        await self.get_access_token()
        WORKSHEET_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets/{worksheet_id}"
        WORKSHEET_DETAIL_URL = WORKSHEET_URL + f"/range(address='{range_rows}')"
        resp = await http_client(
            method="PATCH",
            url=WORKSHEET_DETAIL_URL,
            payload=payload,
            session=self.session,
            access_token=self.access_token,
        )
        return resp


    async def get_data_on_excel_file_by_range(self, range_rows, row_num=None):
        try:
            await self.get_access_token()

            # TODO: build payload for get all file of teams
            response = await self.get_worksheet_detail(
                site_id="aconsvn.sharepoint.com,dcdd5034-9e4b-464c-96a0-2946ecc97a29,eead5dea-f1c3-4008-89e8-f0f7882b734d",
                file_id="01EFHQ6NEXPIGQODOI4ZDYELPV7QFK7HFQ",
                worksheet_id="{B85C4123-37D8-4048-BFF6-4CD980E78699}",
                range_rows=range_rows,
            )

            if ("text" not in response) or (response["text"][0] == None): return None
            if row_num == None: return response["text"][0]

            new_rows = {}
            result = {}
            for idx, value in enumerate(response["text"][0]):
                column = excel_style(None, idx + 1)
                new_rows[column] = value

            result[row_num] = new_rows
            return result
        except Exception as err:
            print(f"Get data on excel file by range failed with: {err}")
            return None
        

async def http_client(url, session, access_token=None, payload=None, method="GET"):
    headers = { "Authorization ": f"Bearer {access_token}" } if access_token else None
    try:
        if method == "PATCH":
            assert payload
            async with session.patch(url, headers=headers, json=payload) as response:
                return await response.json()

        async with session.get(url, headers=headers, data=payload) as response:
            return await response.json()
    except Exception as err:
        print(f"{method} {url} failed with: {err}")
        return None
    

def get_result_in_arr_dict(arr, key, value):
    result = next(
        (dic for dic in arr if dic[key] == value),
        None
    )
    return result


def excel_style(row, col):
    """ Convert given row and column number to an Excel-style cell name. """
    LETTERS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    result = []
    while col:
        col, rem = divmod(col-1, 26)
        result[:0] = LETTERS[rem]
    return ''.join(result)


def convert_date_to_datetime(date):
    min_time = datetime.min.time()
    new_datetime = datetime.combine(date, min_time)
    return new_datetime


def convert_str_to_date_object(raw, is_abb_month=False):
    try:
        # is_abb_month True mean is abbreviated month Jan, Feb, Mar,..., Dec --> 2-July-23
        # else Date of the month 1,2,3,...,31 --> 8/5/22
        if raw is None or raw == "": return ""

        regex = "%d-%b-%Y" if is_abb_month else "%m/%d/%Y"
        date_str = raw[:-2] + f"20{raw[-2:]}"
        date_object = datetime.strptime(date_str, regex)
        return date_object
    except ValueError as err:
        print(f"Convert string to date object failed with: {err}")
        return None


def format_dates_with_excel_style(dates):
    if dates is None: return None

    result = {}
    for idx, value in enumerate(dates):
        column = excel_style(None, idx + 19)
        result[column] = convert_str_to_date_object(value, is_abb_month=True)

    return result


def hash_str_8_dig(raw_str):
    encode = hashlib.sha1(raw_str.encode("utf-8")).hexdigest()
    hash_obj = int(encode, 16) % (10 ** 8)
    return str(hash_obj)


def mapping_cell_with_raw_dates(cell, raw_dates):
    dates = {}
    date_string = ""
    for column, value in cell.items():
        if column in raw_dates and value != "" and value != None:
            date = raw_dates[column]
            dates[date] = value
            date_string = date_string + column + "-" + value + ";"

    return dates, date_string


def split_str_get_key(input_data, char_split):
    if input_data == "" or input_data == None: return "", ""

    results = input_data.split(char_split)
    index_0 = results[0] if len(results) >= 1 else ""
    index_1 = results[1] if len(results) >= 2 else ""

    return index_0, index_1


def frappe_assign(assigns, doctype, name, description=None, priority=None, notify=0):
    add_assignment({
        "assign_to": assigns,
        "doctype": doctype,
        "name": name,
        "description": description,
        "priority": priority,
        "notify": notify
    })

def request_update_A_colum_to_excel(access_token, value, range_num):
    import requests
    import json

    if access_token is None: return None

    head = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token}"}
    url = "https://graph.microsoft.com/v1.0/sites/aconsvn.sharepoint.com,dcdd5034-9e4b-464c-96a0-2946ecc97a29,eead5dea-f1c3-4008-89e8-f0f7882b734d/drive/items/01EFHQ6NEXPIGQODOI4ZDYELPV7QFK7HFQ/workbook/worksheets/{B85C4123-37D8-4048-BFF6-4CD980E78699}"
    url += f"/range(address='A{range_num}')"
    payload = {
        "values" : [[value]],
        "formulas" : [[None]],
        "numberFormat" : [[None]]
    }
    r = requests.patch(url, data=json.dumps(payload), headers=head)
    print(r.status_code)
    print(r.json())


async def handle_get_data_raws(num_start, num_end):
    promises = []
    async with ClientSession() as session:
        msGraph = MSGraph(
            # TODO: implement payload here
            session=session,
            site_name="TEAM 2",
            folder_name="General",
            file_name="pan_planner_test.xlsm",
            worksheet_name="From W1_2023",
        )

        date_row_num = 24
        dates = await msGraph.get_data_on_excel_file_by_range(range_rows=f"S{date_row_num}:OO{date_row_num}")
        date_object = format_dates_with_excel_style(dates=dates)

        for row_num in range(num_start, num_end):
            range_excel_rows = f"A{row_num}:OO{row_num}"
            promise = asyncio.ensure_future(msGraph.get_data_on_excel_file_by_range(row_num=row_num, range_rows=range_excel_rows))
            promises.append(promise)
        row_object = await asyncio.gather(*promises)

        return row_object, date_object, msGraph.access_token


async def handle_update_A_colum_to_excel(data):
    promises = []
    async with ClientSession() as session:
        msGraph = MSGraph(session=session)
        for row_num, hash_key in data.items():
            range_excel_rows = f"A{row_num}"
            payload = {
                "values" : [[hash_key]],
                "formulas" : [[None]],
                "numberFormat" : [[None]]
            }

            # TODO: implement payload here
            promise = asyncio.ensure_future(msGraph.patch_worksheet(
                site_id="aconsvn.sharepoint.com,dcdd5034-9e4b-464c-96a0-2946ecc97a29,eead5dea-f1c3-4008-89e8-f0f7882b734d",
                file_id="01EFHQ6NEXPIGQODOI4ZDYELPV7QFK7HFQ",
                worksheet_id="{B85C4123-37D8-4048-BFF6-4CD980E78699}",
                range_rows=range_excel_rows,
                payload=payload
            ))
            promises.append(promise)

        await asyncio.gather(*promises)
