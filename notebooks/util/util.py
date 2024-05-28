import os
import json
import hashlib
from dateutil import parser
from datetime import datetime

EXCEL_TASK_PRIORITY = {
    "": "Medium",
    "1_Urgen": "Urgent",
    "2_Important": "High",
    "3_Medium": "Medium",
    "4_Low": "Low",
}
EXCEL_TYPE_PARENT_TASK = {
    "": "Other",
    "0_Pre CO": "Pre Concept",
    "1_CO": "Concept",
    "2_BD": "Basic Design",
    "3_DD": "Design Development",
    "4_TD": "Tender Doc",
    "5_CD": "Construction",
    "6_AU": "Authorship",
    "7_Other": "Other",
}
EXCEL_TASK_STATUS = {
    "": "Open",
    "1_Open": "Open",
    "2_In progress": "In Progress",
    "3_Pending": "Pending",
    "4_Cancel": "Cancel",
    "5_Done": "Done",
    "6_Review": "Review",
}
EXCEL_TIME_SHEET_STATUS = {
    "": "Draft",
    "Open": "Draft",
    "In Progress": "Draft",
    "Pending": "Draft",
    "Review": "Draft",
    "Done": "Submitted",
    "Cancel": "Cancelled",
}
EXCEL_TIME_SHEET_DOC_STATUS = {
    "": 0,
    "Draft": 0,
    "Submitted": 1,
    "Cancelled": 2,
}

def mapping_row_with_attr(row_data):
    attrs = {}
    # no need anymore
    attrs["import_key"]          = row_data["A"] if "A" in row_data else ""
    # import_key based on
    # task
    # new_key = f"{payload.expected_start_date};{payload.expected_end_date};{payload.new_end_date}"
    # new_hash_key = hash_str_8_dig(new_key)
    # prev_hash_key, _, __ = split_str_get_key(input_data=payload.prev_hash_key, char_split = "--")
    # timesheet
    # new_key = f"{project_code};{parent_task};{employee_name};{progress};{activity_code};{task};{excel_task_status};{date_string}"
    # new_hash_key = hash_str_8_dig(new_key)
    # A_column_key = f"{new_hash_key}--{task_doc}--{new_time_sheet_doc.name}"
    # prev_hash_key, task_id, time_sheet_id = split_str_get_key(input_data=cell["A"], char_split="--")
    
    attrs["row_type"]            = row_data["B"] if "B" in row_data else ""
    attrs["project_code"]        = row_data["C"] if "C" in row_data else ""
    attrs["project_name"]        = row_data["D"] if "D" in row_data else ""

    
    attrs["expected_start_date"] = parser.parse(row_data["E"]) if "E" in row_data and row_data["E"] != "" else None
    attrs["expected_end_date"]   = parser.parse(row_data["F"]) if "F" in row_data and row_data["F"] != "" else None
    attrs["new_end_date"]        = parser.parse(row_data["G"]) if "G" in row_data and row_data["G"] != "" else None
    
    attrs["phase_name"]          = EXCEL_TYPE_PARENT_TASK[row_data["H"] if "H" in row_data else ""]
    # attrs["phase_name"           = EXCEL_TYPE_PARENT_TASK[row_data["H"] if "H" in row_data else ""]
    # expected_time = float(cell["I"]) if "I" in cell and cell["I"] != "" else 0.0
    attrs["expected_time"]       = float(row_data["I"]) if "I" in row_data and row_data["I"] != "" else 0.0
    # col_J: real time (sum)
    attrs["task_priority"]       = EXCEL_TASK_PRIORITY[row_data["K"] if "K" in row_data else ""]
    attrs["progress"]            = row_data["L"].replace("%", "") if "L" in row_data else ""
    attrs["employee_name"]       = row_data["M"] if "M" in row_data else ""
    attrs["activity_code"]       = row_data["N"] if "N" in row_data else ""
    if attrs["activity_code"] == "":
        attrs["activity_code"] = "0000"
    attrs["task"]                = row_data["O"] if "O" in row_data else ""
    attrs["subject"]             = row_data["O"] if "O" in row_data else ""
    if attrs["subject"] == "":
        attrs["subject"] = "Other"
    attrs["excel_task_status"]   = EXCEL_TASK_STATUS[row_data["P"] if "P" in row_data else ""]
    attrs["task_status"]         = EXCEL_TASK_STATUS[row_data["P"] if "P" in row_data else ""]
    # self.task_number = num
    
    return attrs

def mapping_row_with_time_log(row_data, row_date_header):
    new_date = {}
    date_string = []
    for column, value in row_data.items():
        if column in row_date_header and value != None and value != "":
            date = parser.parse(row_date_header[column])
            new_date[date] = value
            date_string.append(f"{row_date_header[column]}")
    return new_date, date_string

def is_row_empty(row_attrs):
    if row_attrs['project_code'] == "":
        return True
    return False

def is_row_timesheet_empty(row_attrs):
    if row_attrs['project_code'] == "":
        return True
    return False

def is_time_log_row_empty(row_time_logs):
    return True