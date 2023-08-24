import frappe
import asyncio

from aiohttp import ClientSession
from functools import cache

_TENANT_ID = "acfde157-8636-4952-b4e3-ed8fd8e274e9"
_CLIENT_ID = "c9eb157c-a854-4438-aca2-0a72b6866c8f"
_CLIENT_SECRET = "T4E8Q~7fpSTGKCoTxeg0_ss11LJYOaQ-McwRobAi"

@cache
class MSGraph:
    access_token = None

    def __init__(self, session, site_name, folder_name, file_name, worksheet_name):
        self.session = session
        self.site_name = site_name
        self.folder_name = folder_name
        self.file_name = file_name
        self.worksheet_name = worksheet_name


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
        SITES_URL = "https://graph.microsoft.com/v1.0/sites"
        resp = await http_client(url=SITES_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.site_name)
        return result


    async def get_folder(self, site_id):
        FOLDERS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        resp = await http_client(url=FOLDERS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.folder_name)
        return result


    async def get_items_in_folder(self, site_id, folder_id):
        ITEMS_FOLDER_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{folder_id}/children"
        resp = await http_client(url=ITEMS_FOLDER_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.file_name)
        return result


    async def get_worksheet(self, site_id, file_id):
        WORKSHEETS_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets"
        resp = await http_client(url=WORKSHEETS_URL, session=self.session, access_token=self.access_token)
        result = get_result_in_arr_dict(arr=resp["value"], key="name", value=self.worksheet_name)
        return result


    async def get_worksheet_detail(self, site_id, file_id, worksheet_id, range_rows):
        WORKSHEET_URL = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook/worksheets/{worksheet_id}"
        WORKSHEET_DETAIL_URL = WORKSHEET_URL + f"/range(address='{range_rows}')?$select=text"
        result = await http_client(url=WORKSHEET_DETAIL_URL, session=self.session, access_token=self.access_token)
        return result


    async def patch_worksheet(self, site_id, file_id, worksheet_id, range_rows, payload):
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


    async def process_get_rows_excel_file_from_sharepoint(self, row_num, range_rows):
        try:
            if self.access_token is None:
                await self.get_access_token()

            sheet_detail = await self.get_worksheet_detail(
                # TODO:
                site_id="aconsvn.sharepoint.com,dcdd5034-9e4b-464c-96a0-2946ecc97a29,eead5dea-f1c3-4008-89e8-f0f7882b734d",
                file_id="01EFHQ6NEXPIGQODOI4ZDYELPV7QFK7HFQ",
                worksheet_id="{B85C4123-37D8-4048-BFF6-4CD980E78699}",
                range_rows=range_rows,
            )

            if ("text" not in sheet_detail) or (sheet_detail["text"][0] == None):
                return None

            rows = {}
            result = {}
            for idx, value in enumerate(sheet_detail["text"][0]):
                column = excel_style(row_num, idx + 2)
                rows[column] = value

            result[row_num] = rows
            return result
        except Exception as err:
            print(f"Process get row excel file failed with: {err}")
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


def convert_date(raw):
    from datetime import datetime
    if raw is None or raw == "": return ""

    date_str = raw[:-2] + f"20{raw[-2:]}"
    date_object = datetime.strptime(date_str, '%m/%d/%Y')
    return date_object


def frappe_assign(email, doctype, docname):
    from frappe.desk.form import assign_to
    assign_to.add({
        "assign_to": email,
        "doctype": doctype,
        "name": docname,
        "description": None,
        "priority": None,
        "notify": 0
    })


async def get_tasks_from_excel(num_start, num_end):
    promises = []
    async with ClientSession() as session:
        msGraph = MSGraph(
            # TODO:
            session=session,
            site_name="TEAM 2",
            folder_name="General",
            file_name="pan_planner_test.xlsm",
            worksheet_name="From W1_2023",
        )

        for row_num in range(num_start, num_end):
            range_excel_rows = f"B{row_num}:R{row_num}"
            promise = asyncio.ensure_future(msGraph.process_get_rows_excel_file_from_sharepoint(row_num=row_num, range_rows=range_excel_rows))
            promises.append(promise)

        responses = await asyncio.gather(*promises)
        return responses


async def process_handler_insert_task():
    TASK_REQUIRED_COLUMN = ["B","C","E","F","L","M","N","O","P"]
    TASK_PRIORITY = { "": "",
                     "1_Urgen": "Urgent",
                     "2_Important": "High",
                     "3_Medium": "Medium",
                     "7_Transfer": "Medium" }
    TASK_STATUS = { "": "Open",
                   "10%": "Working",
                   "20%": "Working",
                   "30%": "Working",
                   "50%": "Working",
                   "70%": "Working",
                   "80%": "Working",
                   "100%": "Completed" }
    
    # TEAM 2: 209 -> 3000
    tasks = await get_tasks_from_excel(num_start=209, num_end=300)
    for task in tasks:
        if task is None: continue

        for row_num in task:
            rows = task[row_num]
            if rows is None: continue

            map_rows = list(map(rows.get, TASK_REQUIRED_COLUMN))
            if "Pa" in map_rows or map_rows[-1] == "": continue
            
            status = TASK_STATUS[map_rows[5]]
            priority = TASK_PRIORITY[map_rows[4]]
            progress = map_rows[5].replace("%", "")
            exp_start_date = convert_date(map_rows[2])
            exp_end_date = convert_date(map_rows[3])

            task_doc = frappe.new_doc("Task")
            task_doc.custom_no = row_num
            task_doc.subject = map_rows[-1]
            task_doc.project = map_rows[1]
            task_doc.status = status
            task_doc.priority = priority
            task_doc.parent_task = None
            task_doc.exp_start_date = exp_start_date
            task_doc.exp_end_date = exp_end_date
            task_doc.progress = progress

            if status == "Completed":
                task_doc.completed_on = exp_end_date
            
            task_doc.insert()

            if map_rows[6] != "":
                user_id = frappe.db.get_value("Employee", {"employee_name": map_rows[6]}, ["user_id"])
                if user_id:
                    frappe_assign(email=user_id, doctype=task_doc.doctype, docname=task_doc.subject)
