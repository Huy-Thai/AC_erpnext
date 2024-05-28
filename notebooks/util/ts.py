class Ts():
    project_doc_list = {}
    task_doc_list = {}
    employee_doc_list = {}
    timesheet_doc_list = {}
    conn = None

    def __init__(self, conn):
        self.conn = conn
        
    def get_project_doc(self, doc_name):
        # return if cached else get db & cache
        if self.project_doc_list.get(doc_name):
            return self.project_doc_list[doc_name]
        else:
            doc = self.conn.get_doc('Project', doc_name)
            if doc:
                self.project_doc_list[doc_name] = doc
                return doc
    
        return None
    
    def get_employee_doc(self, doc_name):
        # return if cached else get db & cache
        if self.employee_doc_list.get(doc_name):
            return self.employee_doc_list[doc_name]
        else:
            doc = self.conn.get_doc('Employee', doc_name)
            if doc:
                self.employee_doc_list[doc_name] = doc
                return doc
        return None
    
    def get_employee_doc_by_name(self, employee_name):
        doc_name = self.conn.get_value("Employee", 
                                  ["name"],
                                  [["employee_name", "=", employee_name]])
        print(f"employee: {doc_name}")
        if doc_name:
            return self.get_employee_doc(doc_name['name'])
            
        return None
    
    def get_task(self, doc_name):
        # return if cached else get db & cache
        if self.task_doc_list.get(doc_name):
            return self.task_doc_list[doc_name]
        else:
            doc = self.conn.get_doc('Task', doc_name)
            if doc:
                self.task_doc_list[doc_name] = doc
                return doc
        return None

    def get_task_doc_by_fields(self, payload={}):

        if (payload["subject"] == "" or
            payload["project_code"] == "" or
            payload["phase_name"] == ""):
            print(f"Cannot get task doc due fields empty")
            return None
        
        filters = [["subject", "=", payload["subject"]],
                   ["project", "=", payload["project_code"]], 
                   # ["type", "=", payload["phase_name"]],
                   ["custom_activity", "=", payload["activity_code"]],
                   ["is_group", "=", payload["is_group"]]]
        if payload.get("parent_task"):
            filters.append(["parent_task", "=", payload["parent_task"]])
        
        doc_name = self.conn.get_value("Task", ["name"], filters)
        if doc_name:
            # print(f"doc_name existed")
            return self.get_task(doc_name["name"])
        return None

    def get_timesheet(self, doc_name):
        # return if cached else get db & cache
        if self.timesheet_doc_list.get(doc_name):
            return self.timesheet_doc_list[doc_name]
        else:
            doc = self.conn.get_doc('Timesheet', doc_name)
            if doc:
                self.timesheet_doc_list[doc_name] = doc
                return doc
        return None

    def get_timesheet_name_by_fields(self, payload={}):
        doc_name = self.conn.get_value("Timesheet", ["name"],
                                      [["employee", "=", payload['employee_code']],
                                       ["parent_project", "=", payload['project_code']],
                                       ["custom_task", "=", payload['task']],
                                       ["custom_parent_task", "=", payload['parent_task']],
                                       ["custom_timelog", "=", payload['timelog']],
                                       # ["custom_phase", "=", payload['phase_name']],
                                       ["custom_activity", "=", payload["activity_code"]]])
        
        return doc_name

    def create_task(self, payload = {}):
        doc = {
            "doctype": "Task"
        }
        doc["subject"] = payload["subject"]
        doc["project"] = payload["project_code"]
        doc["type"] = payload["phase_name"]
        doc["custom_activity"] = payload["activity_code"]
        doc["is_group"] = payload["is_group"]
        doc["progress"] = payload["progress"]
        doc["status"] = payload["task_status"]
        if payload.get("parent_task"):
            doc["parent_task"] = payload["parent_task"]
        
        return self.conn.insert(doc)
        # pass
    def update_task(self, doc, payload):
        payload["doctype"] = "Task"
        payload["name"] = doc["name"]
        return self.conn.update(payload)
        

    def create_timesheet(self, payload):
        self.conn.insert(payload)