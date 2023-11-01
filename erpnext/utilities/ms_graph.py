import json
import frappe
import asyncio
import hashlib

from datetime import datetime
from aiohttp import ClientSession
from functools import cache

_TENANT_ID = "acfde157-8636-4952-b4e3-ed8fd8e274e9"
_CLIENT_ID = "c9eb157c-a854-4438-aca2-0a72b6866c8f"
_CLIENT_SECRET = "T4E8Q~7fpSTGKCoTxeg0_ss11LJYOaQ-McwRobAi"
_CACHE_EXPIRED = 432000 # 5 day

EXCEL_PARENT_TASK = { "": None, "0_Pre CO": "31c7e0fca6", "1_CO": "8c0ce783ba", "2_BD": "b493e1ee7e", "3_DD": "ca095b7007", "4_TD": "0216871bc3", "5_CD": "78079e408a", "6_AU": "7b281811b0", "7_Other": "e4705bc9f2" }
EXCEL_TASK_PRIORITY = { "": "Medium", "1_Urgen": "Urgent", "2_Important": "High", "3_Medium": "Medium", "4_Low": "Low" }
EXCEL_TASK_STATUS = { "": "Open", "1_Open": "Open", "2_In progress": "In Progress", "3_Pending": "Pending", "4_Cancel": "Cancel", "5_Done": "Done", "6_Review": "Review" }
EXCEL_TIME_SHEET_STATUS = { "": "Draft", "Open": "Draft", "In Progress": "Draft", "Done": "Completed", "Cancel": "Cancelled" }
TIME_SHEET_STATUS_CANCEL_UPDATE = ["Completed", "Cancelled", "Submitted"]


class TaskModel:
    def __init__(self, num, cell):
        assert cell["C"] != "", "Value Cell C is required"
        assert cell["O"] != "", "Value Cell O is required"

        expected_time = float(cell["I"]) if cell["I"] != '' else 0.0
        task_status = EXCEL_TASK_STATUS[cell["P"]]
        task_priority = EXCEL_TASK_PRIORITY[cell["K"]]
        parent_task = EXCEL_PARENT_TASK[cell["H"]]

        self.task_number = num
        self.subject = cell["O"]
        self.project = cell["C"]
        self.status = task_status
        self.priority = task_priority
        self.progress = cell["L"].replace("%", "")
        self.expected_time = expected_time
        self.employee_name = cell["M"]
        self.parent_task = parent_task


@cache
class MSGraph:
    access_token = None

    def __init__(self, session, site_name=None, folder_name=None, file_name=None, worksheet_name=None):
        self.session = session
        self.site_name = site_name
        self.folder_name = folder_name
        self.file_name = file_name
        self.worksheet_name = worksheet_name
        self.frappe_cache = frappe.cache()


    async def get_access_token(self):
        cache_key = "access_token"
        if result_cache := self.frappe_cache.get_value(cache_key):
            self.access_token = result_cache
            return

        AUTH_URL = f"https://login.microsoftonline.com/{_TENANT_ID}/oauth2/v2.0/token"
        PAYLOAD = {
            "grant_type": "client_credentials",
            "client_id": _CLIENT_ID,
            "scope": "https://graph.microsoft.com/.default",
            "client_secret": _CLIENT_SECRET,
        }
        resp = await http_client(url=AUTH_URL, session=self.session, payload=PAYLOAD)
        self.access_token = resp["access_token"] if resp else None
        self.frappe_cache.set_value(cache_key, self.access_token, expires_in_sec=2700) # 45 minutes
        return


    async def get_site(self):
        assert self.site_name != None, "Param site_name is required"
        if result_cache := self.frappe_cache.get_value(self.site_name):
            return json.loads(result_cache)

        SITES_URL = "https://graph.microsoft.com/v1.0/sites"
        resp = await http_client(url=SITES_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.site_name)
        self.frappe_cache.set_value(self.site_name, json.dumps(result), expires_in_sec=_CACHE_EXPIRED)
        return result


    async def get_folder(self, site_id):
        assert self.folder_name != None and site_id != None, "Param folder_name and site_id are required"
        cache_key = f'{site_id}_{self.folder_name}'
        if result_cache := self.frappe_cache.get_value(cache_key):
            return json.loads(result_cache)

        FOLDERS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        resp = await http_client(url=FOLDERS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.folder_name)
        self.frappe_cache.set_value(cache_key, json.dumps(result), expires_in_sec=_CACHE_EXPIRED)
        return result


    async def get_items_in_folder(self, site_id, folder_id):
        assert self.file_name != None and site_id != None and folder_id != None, "Param file_name and site_id and folder_id are required"
        cache_key = f'{site_id}_{folder_id}_{self.file_name}'
        if result_cache := self.frappe_cache.get_value(cache_key):
            return json.loads(result_cache)

        ITEMS_FOLDER_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{folder_id}/children"
        resp = await http_client(url=ITEMS_FOLDER_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.file_name)
        self.frappe_cache.set_value(cache_key, json.dumps(result), expires_in_sec=_CACHE_EXPIRED)
        return result


    async def get_worksheet(self, site_id, file_id):
        assert self.worksheet_name != None and file_id != None and site_id != None, "Param worksheet_name and file_id and site_id are required"
        cache_key = f'{site_id}_{file_id}_{self.worksheet_name}'
        if result_cache := self.frappe_cache.get_value(cache_key):
            return json.loads(result_cache)

        WORKSHEETS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets"
        resp = await http_client(url=WORKSHEETS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.worksheet_name)
        self.frappe_cache.set_value(cache_key, json.dumps(result), expires_in_sec=_CACHE_EXPIRED)
        return result


    async def get_worksheet_detail(self, site_id, file_id, worksheet_id, range_rows):
        assert worksheet_id != None and file_id != None and site_id != None, "Param worksheet_id and file_id and site_id are required"
        WORKSHEET_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets/{worksheet_id}"
        WORKSHEET_DETAIL_URL = WORKSHEET_URL + f"/range(address='{range_rows}')?$select=text"
        result = await http_client(url=WORKSHEET_DETAIL_URL, session=self.session, access_token=self.access_token)
        return result


    async def patch_worksheet(self, site_id, file_id, worksheet_id, range_rows, payload):
        assert worksheet_id != None and file_id != None and site_id != None, "Param worksheet_id and file_id and site_id are required"
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


    async def get_data_on_excel_file_by_range(self, body, range_rows, row_num=None):
        try:
            await self.get_access_token()

            response = await self.get_worksheet_detail(
                site_id=body['site_id'],
                file_id=body['file_id'],
                worksheet_id=body['worksheet_id'],
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
        column = excel_style(None, idx + 17)
        result[column] = convert_str_to_date_object(value, is_abb_month=True)

    return result


def hash_str_8_dig(raw_str):
    encode = hashlib.sha1(raw_str.encode("utf-8")).hexdigest()
    hash_obj = int(encode, 16) % (10 ** 8)
    return str(hash_obj)


def mapping_cell_with_dates_raw(cell, dates_raw):
    dates = {}
    date_string = ""
    for column, value in cell.items():
        if column in dates_raw and value != "" and value != None:
            date = dates_raw[column]
            dates[date] = value
            date_string = date_string + column + "-" + value + ";"

    return dates, date_string


def split_str_get_key(input_data, char_split):
    if input_data == "" or input_data == None: return "", ""

    results = input_data.split(char_split)
    index_0 = results[0] if len(results) >= 1 else ""
    index_1 = results[1] if len(results) >= 2 else ""

    return index_0, index_1


async def handle_get_data_raws(body_query, num_start, num_end, date_row_num):
    promises = []
    async with ClientSession() as session:
        msGraph = MSGraph(
            session=session,
            site_name=None,
            folder_name=None,
            file_name=None,
            worksheet_name=None,
        )

        dates = await msGraph.get_data_on_excel_file_by_range(body=body_query, range_rows=f"Q{date_row_num}:KZ{date_row_num}")
        date_object = format_dates_with_excel_style(dates=dates)

        for row_num in range(num_start, num_end):
            range_excel_rows = f"A{row_num}:KZ{row_num}"
            promise = asyncio.ensure_future(msGraph.get_data_on_excel_file_by_range(body=body_query, row_num=row_num, range_rows=range_excel_rows))
            promises.append(promise)
        row_object = await asyncio.gather(*promises)

        return row_object, date_object, msGraph.access_token


def update_column_excel_file(access_token, body_query, range_num, value):
    import requests
    try:
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token}"}
        url = f"https://graph.microsoft.com/v1.0/sites/{body_query['site_id']}/drive/items/{body_query['file_id']}/workbook/worksheets/{body_query['worksheet_id']}"
        url += f"/range(address='A{range_num}')"
        payload = {
            "values" : [[value]],
            "formulas" : [[None]],
            "numberFormat" : [[None]]
        }

        response = requests.patch(url, data=json.dumps(payload), headers=headers)
        return response
    except Exception as err:
        print(f"Update column on excel file failed with: {err}")
        return None
