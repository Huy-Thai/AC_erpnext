frappe.listview_settings["Project"] = {
	add_fields: ["status", "priority", "is_active", "percent_complete", "expected_end_date", "project_name"],
	filters:[["status","=", "Opened"]],
	get_indicator: function(doc) {
		if(doc.status=="Opened" && doc.percent_complete) {
			return [__("{0}%", [cint(doc.percent_complete)]), "orange", "percent_complete,>,0|status,=,Opened"];
		} else {
			return [__(doc.status), frappe.utils.guess_colour(doc.status), "status,=," + doc.status];
		}
	},
};
