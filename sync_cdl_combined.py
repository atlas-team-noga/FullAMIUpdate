"""
Module for synchronizing ServiceNow and Jira dropdown options with a cascade structure.

This module retrieves data from ServiceNow JSON files and maps them into a hierarchical
structure based on parents and children. The resulting structure is then compared with
the existing Jira dropdown structure, and differences are identified. These differences
are logged in a CSV report and can be addressed either in diagnostic mode or applied
directly to Jira.

Additionally, the module supports physical deletion of specific services marked as
"Planned" in ServiceNow, as well as adjustments to the status of new or existing
dropdown options.

Configuration is handled via predefined authentication settings, endpoints, and file
paths.

Attributes:
    JIRA_BASE_URL (str): Base URL of the Jira instance.
    CUSTOM_FIELD_ID (str): Identifier for the custom field in Jira.
    CONTEXT_ID (str): Context ID for the custom field in Jira.
    SESSION (requests.Session): Authenticated HTTP session for API requests.
    MODE (str): Running mode for the script (e.g., "diagnostic" or "apply").
    DISABLE_OBSOLETE (bool): Whether to disable obsolete entries.
    REACTIVATE_ACTIVE (bool): Whether to reactivate active entries.
    CSV_REPORT (str): File path for the generated CSV report.
"""
import json
import requests
import csv
import os
from datetime import datetime
from config_utils import AUTH, HEADERS, JIRA_URL

# --- Configuration ---
JIRA_BASE_URL = JIRA_URL
CUSTOM_FIELD_ID = "customfield_12690"
CONTEXT_ID = "13122"

SESSION = requests.Session()
SESSION.auth = AUTH
SESSION.headers.update({"Content-Type": "application/json"})

# ✅ Options
MODE = os.getenv("SYNC_MODE", "diagnostic")  # Default diagnostic, or retrieves the choice from main_workflow
DISABLE_OBSOLETE = True
REACTIVATE_ACTIVE = True
CSV_REPORT = "sync_report.csv"

# --- Utility functions ---
def normalize_name(name_norm):
    """
    Removes leading and trailing whitespace from a string and converts it to lowercase.

    This function standardizes the format of the input string by ensuring that it is stripped
    of any unnecessary whitespace and is entirely in lowercase letters.

    :param name_norm: The string to be normalized.
    :type name_norm: str
    :return: A normalized string that has been stripped of whitespace and converted to lowercase.
    :rtype: str
    """
    return name_norm.strip().lower()

def api_call(method, url, payload_call=None):
    """
    Makes an API call using the specified HTTP method, URL, and optional payload. The function supports
    a diagnostic mode, where it simulates an API call and always returns a status code of 200 and
    an empty dictionary without making a real HTTP request.

    If the function is not in diagnostic mode, it sends an HTTP request using the provided method and URL
    and optional payload. It logs the details of the request and response, and returns the response
    status code and payload.

    :param method: The HTTP method for the request (e.g., 'GET', 'POST', 'PUT', 'DELETE').
    :type method: str
    :param url: The URL to which the API call is made.
    :type url: str
    :param payload_call: The JSON payload to include in the request, if applicable (optional).
    :type payload_call: dict or None
    :return: A tuple containing the HTTP status code of the response and the parsed JSON payload
        from the response, or an empty dictionary if there was no response content.
    :rtype: tuple[int, dict]
    """
    if MODE == "diagnostic":
        return 200, {}
    print(f"\n➡️ API CALL: {method} {url}")
    if payload_call:
        print(f"Payload:\n{json.dumps(payload_call, indent=2)}")
    r = SESSION.request(method, url, json=payload_call)
    print(f"⬅️ Response: {r.status_code} {r.text if r.text else '[No Content]'}")
    if r.status_code not in [200, 201, 204]:
        print(f"❌ API Error: {r.status_code} - {r.text}")
    return r.status_code, r.json() if r.text else {}

def fetch_all_options():
    """
    Fetches all the options available for a specific JIRA custom field context.

    This function communicates with the JIRA REST API to retrieve all values associated
    with a custom field context in paginated requests. It iteratively fetches data until
    all available options are retrieved.

    :return: A list of options associated with the specified JIRA custom field context.
    :rtype: list
    """
    all_options = []
    start_at = 0
    max_results = 100
    url_base = f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option"
    while True:
        url = f"{url_base}?startAt={start_at}&maxResults={max_results}"
        response = SESSION.get(url)
        response.raise_for_status()
        data_opt = response.json()
        options = data_opt.get("values", [])
        all_options.extend(options)
        if data_opt.get("isLast", True):
            break
        start_at += max_results
    return all_options

def log_full(parent, child_log, status_log):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(CSV_REPORT, "a", newline="", encoding="utf-8") as f_log:
        writer_log = csv.writer(f_log)
        writer_log.writerow([timestamp, MODE, parent, child_log, status_log])

# --- Load ServiceNow data ---
with open("u_cmdb_ci_business_domain.json", "r", encoding="utf-8") as f:
    business_domains = json.load(f)
with open("cmdb_ci_service.json", "r", encoding="utf-8") as f:
    services = json.load(f)

if isinstance(business_domains, dict):
    business_domains = business_domains.get("result", business_domains.get("records", []))
if isinstance(services, dict):
    services = services.get("result", services.get("records", []))

# ✅ Build the target structure
domain_map = {d["sys_id"]: d["name"] for d in business_domains if d.get("sys_id") and d.get("name")}
cascade_structure = {}
planned_services = set()  # ✅ Storage of "Planned" services for physical deletion

for service in services:
    parent_name = domain_map.get(service.get("u_business_domain"))
    if parent_name:
        status = str(service.get("operational_status"))
        name = service.get("name")

        # ✅ FIX: If status is '20' (Planned), exclude it from the target structure
        # and mark it for physical deletion from the Jira dropdown.
        if status == "20":
            planned_services.add(normalize_name(name))
            continue

        if parent_name not in cascade_structure:
            cascade_structure[parent_name] = []

        # A service is disabled only if it is 'Retired' (6)
        cascade_structure[parent_name].append({
            "name": name,
            "disabled": (status == "6")
        })

# Alphabetical sorting of parents and children
cascade_structure = {k: sorted(v, key=lambda x: x["name"].lower()) for k, v in sorted(cascade_structure.items())}

print(f"✅ Target structure built: {len(cascade_structure)} parents identified.")

# --- Retrieve current Jira structure ---
current_options = fetch_all_options()
current_structure = {}
parent_map = {opt["id"]: opt["value"] for opt in current_options if "optionId" not in opt}
for opt in current_options:
    val = opt["value"]
    if "optionId" not in opt:
        current_structure[val] = {"id": opt["id"], "children": []}
    else:
        parent_val = parent_map.get(opt["optionId"])
        if parent_val:
            current_structure[parent_val]["children"].append({
                "name": val,
                "disabled": opt.get("disabled", False),
                "id": opt.get("id")
            })

# Alphabetical sorting of current parents and children
current_structure = {k: {"id": v["id"], "children": sorted(v["children"], key=lambda x: x["name"].lower())} for k, v in
                     sorted(current_structure.items())}

print(f"✅ Current Jira structure: {len(current_structure)} parents found.")

# ✅ PRELIMINARY STEP: Physical deletion of "Planned (20)" services present in Jira
print(f"🗑️ Searching for 'Planned (20)' services to remove from dropdown...")
for parent_name, data in current_structure.items():
    for child in data["children"]:
        if normalize_name(child["name"]) in planned_services:
            print(f"🔥 Deleting {child['name']} under {parent_name} (Status 20 detected)")
            log_full(parent_name, child["name"], "DELETED (Status 20)")
            if MODE == "apply":
                delete_url = f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option/{child['id']}"
                api_call("DELETE", delete_url)

# --- Prepare CSV report ---

with open(CSV_REPORT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Date/Time", "Mode", "Parent", "Child", "Status"])

# --- Diagnostic or Apply ---
to_add, to_move, to_disable, to_reactivate = [], [], [], []

for parent_name, children in cascade_structure.items():
    parent_name_norm = normalize_name(parent_name)
    jira_parent_key = next((p for p in current_structure.keys() if normalize_name(p) == parent_name_norm), None)

    if not jira_parent_key:
        print(f"⚠ Parent {parent_name} missing in Jira.")
        log_full(parent_name, "", "Parent missing")
        continue

    jira_children = current_structure[jira_parent_key]["children"]

    for child in children:
        child_norm = normalize_name(child["name"])
        found_in = None
        child_id = None

        for cur_parent, data in current_structure.items():
            for cur_child in data["children"]:
                if normalize_name(cur_child["name"]) == child_norm:
                    found_in = cur_parent
                    child_id = cur_child["id"]
                    break
            if found_in:
                break

        # Addition
        if not found_in:
            to_add.append((parent_name, child["name"]))
            log_full(parent_name, child["name"], "To add")
            if MODE == "apply":
                payload = {
                    "options": [
                        {
                            "value": child["name"],
                            "disabled": child["disabled"],
                            "optionId": current_structure[jira_parent_key]["id"]
                        }
                    ]
                }
                api_call("POST", f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option", payload)

        # Move
        elif normalize_name(found_in) != parent_name_norm:
            to_move.append((child["name"], found_in, parent_name))
            log_full(parent_name, child["name"], "To move")
            if MODE == "apply":
                url_disable = f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option/{child_id}"
                api_call("PUT", url_disable, {"disabled": True})

                payload_add = {
                    "options": [
                        {
                            "value": child["name"],
                            "disabled": child["disabled"],
                            "optionId": current_structure[jira_parent_key]["id"]
                        }
                    ]
                }
                api_call("POST", f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option", payload_add)

        # Reactivation
        else:
            jira_child = next(c for c in jira_children if normalize_name(c["name"]) == child_norm)
            if jira_child.get("disabled", False) and not child["disabled"]:
                to_reactivate.append((parent_name, child["name"]))
                log_full(parent_name, child["name"], "To reactivate")
                if MODE == "apply" and REACTIVATE_ACTIVE:
                    url_reactivate = f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option/{jira_child['id']}"
                    api_call("PUT", url_reactivate, {"disabled": False})
            else:
                log_full(parent_name, child["name"], "OK")

# Disable obsolete
for parent_name, data in current_structure.items():
    target_children = [normalize_name(c["name"]) for c in cascade_structure.get(parent_name, [])]
    for child in data["children"]:
        if normalize_name(child["name"]) not in target_children and not child.get("disabled", False):
            to_disable.append((parent_name, child["name"]))
            log_full(parent_name, child["name"], "To disable")
            if MODE == "apply" and DISABLE_OBSOLETE:
                url_disable = f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option/{child['id']}"
                api_call("PUT", url_disable, {"disabled": True})
        else:
            if normalize_name(child["name"]) in target_children:
                log_full(parent_name, child["name"], "OK")

# Reordering options in Jira
if MODE == "apply":
    for parent_name, data in current_structure.items():
        sorted_ids = [child["id"] for child in data["children"]]
        if sorted_ids:
            payload_move = {
                "customFieldOptionIds": sorted_ids,
                "position": "First"
            }
            print(f"\n🔄 Reordering children for {parent_name}")
            api_call("PUT", f"{JIRA_BASE_URL}/rest/api/3/field/{CUSTOM_FIELD_ID}/context/{CONTEXT_ID}/option/move", payload_move)

# --- Display diagnostic ---
print("\n📋 Complete diagnostic:")
print(f"➕ To add ({len(to_add)}): {to_add}")
print(f"🔄 To move ({len(to_move)}): {to_move}")
print(f"🛑 To disable ({len(to_disable)}): {to_disable}")
print(f"✅ To reactivate ({len(to_reactivate)}): {to_reactivate}")
print(f"\n✨ Mode: {MODE.upper()} finished. CSV report generated: {CSV_REPORT}")
