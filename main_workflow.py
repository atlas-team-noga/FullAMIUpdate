"""
Main module for managing business domain orchestration, cascading dropdown
synchronization, and JIRA issue updates.

This module organizes various phases of automation, including business
domain detection and processing, external dropdown synchronization, and the
synchronization of cascading dropdown options within JIRA issues. Logging is
set up for tracking workflows and cascading dropdown synchronization tasks.

Classes, constants, and functions provided in this module address different
aspects of automation, following configured project settings.

"""
import time
import subprocess
import sys
import os
import requests
import json
import logging

from business_domain import (
    search_issues_missing_business_domain,
    get_unique_business_domain_ids,
    load_automation_rules_table_from_confluence,
    get_business_domain_info_from_json,
    update_confluence_table_with_new_domain
)
from logger_utils import format_duration
from config_utils import (
    JIRA_URL,
    AUTH,
    HEADERS,
    get_project_config,
    DRY_RUN,
)
from jira_utils import update_issue
from IssuesProcessing import sync_business_services_from_json

# === CONFIGURATION ===
DOMAIN_JSON_FILE = "u_cmdb_ci_business_domain.json"
SERVICE_JSON_FILE = "cmdb_ci_service.json"

project_cfg = get_project_config()
PROJECT_KEY = project_cfg["project_key"]
ISSUE_TYPE = project_cfg["issue_type"]
CUSTOM_FIELDS = project_cfg["custom_fields"]

CASCADING_DROPDOWN_FIELD_ID = project_cfg["dropdown_cascading_field"]["bus_domain_and_service"]
BUSINESS_SERVICE_OPTION_ID = CUSTOM_FIELDS.get("business_service_option_id", "customfield_12822")

# === LOGGING SETUP ===
workflow_logger = logging.getLogger('workflow')
workflow_logger.setLevel(logging.INFO)
workflow_handler = logging.FileHandler('main_workflow.log', encoding='utf-8')
workflow_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
workflow_logger.addHandler(workflow_handler)

cascading_logger = logging.getLogger('cascading_sync')
cascading_logger.setLevel(logging.INFO)
cascading_handler = logging.FileHandler('cascading_dropdown_sync.log', encoding='utf-8')
cascading_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
cascading_logger.addHandler(cascading_handler)

def log_workflow(message, level="info"):
    """
    Logs a workflow-related message at a specified logging level. This is a utility function
    designed for capturing and recording key workflow events in an application.

    :param message: The log message to be recorded.
    :type message: str
    :param level: The severity level of the log message. Defaults to "info".
                   Must correspond to a valid method in the logger.
    :type level: str, optional
    :return: None
    """
    print(message)
    getattr(workflow_logger, level)(message)

def log_cascading(message, level="info"):
    """
    Logs a message using a cascading logger and prints it to the console.

    This function is designed to send a log message to a cascading logger based on
    the specified log level. By default, the log level is set to "info". It also
    prints the message to the console for immediate visibility.

    :param message: The message to be logged. Must be a descriptive string providing
        the context or information to be logged.
    :type message: str
    :param level: The severity level of the message. Must match one of the valid
        levels supported by the cascading logger (e.g., "info", "error", "warning", etc.).
        Defaults to "info".
    :type level: str, optional
    :return: None
    """
    print(message)
    getattr(cascading_logger, level)(message)

def reset_log_files():
    """
    Clears the content of a predefined set of log files.

    This function iterates through a list of predefined log file names and clears
    their contents by opening each file in write mode and writing an empty string.

    :raises Exception: If any file operation fails, an exception is silently caught,
        and the process continues for the remaining files.
    :return: None
    """
    log_files = ['main_workflow.log', 'cascading_dropdown_sync.log', 'business_service_automation.log', 'business_service_errors.log']
    for log_file in log_files:
        try:
            with open(log_file, 'w', encoding='utf-8') as f: f.write('')
        except Exception: pass

# ============================================================================
# PHASE 1 : BUSINESS DOMAIN ORCHESTRATION
# ============================================================================

def check_for_new_business_domains():
    """
    Checks for new business domains by performing a search and logs the workflow status.

    This function logs the process of searching for new business domains, checking if any
    issues are missing business domain information, and identifies unique business domain
    IDs if any new domains are detected.

    :returns: A tuple containing a boolean flag indicating whether new business domains are
              detected and a set of unique identifiers for the new business domains.
    :rtype: tuple[bool, set]
    """
    log_workflow("\n" + "="*80)
    log_workflow("=== STEP 1: Checking for New Business Domains ===")
    log_workflow("="*80)
    issues = search_issues_missing_business_domain()
    if not issues:
        log_workflow("✓ No new Business Domain detected")
        return False, set()
    unique_ids = get_unique_business_domain_ids(issues)
    log_workflow(f"⚠️  NEW BUSINESS DOMAINS DETECTED: {len(unique_ids)}")
    return True, unique_ids

def process_new_business_domains(domain_ids, domains_data, automation_table):
    """
    Processes new business domains by iterating through the given list of domain IDs,
    checking if they exist in the automation table, and adding them if not already present.
    The method also updates the Confluence table with new domain details when necessary.

    :param domain_ids: A list or iterable of IDs representing business domains to be processed.
    :type domain_ids: list
    :param domains_data: A dictionary or JSON object containing details about business domains
        keyed by their IDs.
    :type domains_data: dict
    :param automation_table: A dictionary or lookup structure to verify if a domain ID
        exists and has already been processed.
    :type automation_table: dict
    :return: The number of business domains successfully processed and/or added to Confluence.
    :rtype: int
    """
    log_workflow("\n" + "="*80)
    log_workflow("=== STEP 2: Processing New Business Domains ===")
    log_workflow("="*80)
    processed = 0
    for d_id in sorted(domain_ids):
        if d_id in automation_table:
            processed += 1
        else:
            info = get_business_domain_info_from_json(d_id, domains_data)
            if info and update_confluence_table_with_new_domain(info):
                log_workflow(f" ✓ Added to Confluence: {info.get('name')}")
                processed += 1
    return processed

def load_domain_data(json_file):
    """
    Loads domain data from a specified JSON file.

    This function attempts to load and parse the content of the specified JSON
    file. If the file does not exist or an error occurs during file reading or
    parsing, the function will safely return None.

    :param json_file: The path to the JSON file to be loaded and parsed.
    :type json_file: str
    :return: The parsed content of the JSON file as a dictionary, or None if the
             file does not exist or an error occurs during processing.
    :rtype: dict or None
    """
    if not os.path.exists(json_file): return None
    try:
        with open(json_file, 'r', encoding='utf-8') as f: return json.load(f)
    except: return None

# ============================================================================
# PHASE 2 : EXTERNAL DROPDOWN SYNC
# ============================================================================

def sync_cascading_dropdown_from_json_files():
    """
    Synchronizes cascading dropdown data from JSON files by invoking an external script.

    This function runs a subprocess for executing a Python script designed to synchronize
    dropdown data. The behavior of the synchronization process is determined by the global
    DRY_RUN configuration, which allows the function to operate in either "diagnostic"
    (non-destructive) or "apply" (destructive) mode. Logs are captured during the process
    for both workflows and dropdown synchronization events.

    :raises Exception: If there is an issue running the subprocess or during the
        synchronization process.

    :return: A dictionary with the synchronization result. This includes a "status" key with
        value `success` if the operation completes without errors, or an "error" key with
        a description in case of failure.
    :rtype: dict
    """
    log_workflow("\n" + "=" * 80)
    log_workflow("=== PHASE 2: Cascading Dropdown Synchronization (External) ===")
    log_workflow("=" * 80)

    # Automatically use mode based on global DRY_RUN configuration
    mode_arg = "diagnostic" if DRY_RUN else "apply"
    log_workflow(f"Running in mode: {mode_arg.upper()}")

    try:
        process = subprocess.Popen(
            [sys.executable, "sync_cdl_combined.py"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8',
            env={**os.environ, "SYNC_MODE": mode_arg}
        )
        for line in process.stdout: log_cascading(f"  [Dropdown Sync] {line.strip()}")
        process.wait()
        return {'status': 'success'} if process.returncode == 0 else {'error': 'Failed'}
    except Exception as e: return {'error': str(e)}

# ============================================================================
# PHASE 3 : JIRA ISSUES UPDATES
# ============================================================================

def get_non_retired_business_services():
    """
    Fetches non-retired business services from a Jira project.

    This function constructs a JQL query to retrieve all Jira issues of a specified
    type associated with business services that are not marked as "Retired".
    The query also includes issues where the business service status field is
    empty. The `fetch_jira_issues_paginated` utility is used to fetch the results
    in a paginated manner.

    :return: A list of Jira issues representing non-retired business services.
    :rtype: list
    """
    status_field = CUSTOM_FIELDS["business_service_status"]
    jql = f'project = {PROJECT_KEY} AND issuetype = "{ISSUE_TYPE}" AND ("{status_field}" != "Retired (6)" OR "{status_field}" is EMPTY)'
    from jira_utils import fetch_jira_issues_paginated
    return fetch_jira_issues_paginated(jql)

def get_cascading_field_options(field_id):
    """
    Retrieves cascading field options for a specified field in JIRA. Constructs a tree-like dictionary
    structure to represent relationships between parent and child options for the cascading field.

    :param field_id: The unique identifier of the JIRA field whose cascading options are to be retrieved
        (str)

    :return: A dictionary representing the cascading field options. The keys are parent option names,
        and the values are dictionaries containing the parent option's ID and its child options as
        nested dictionaries. An empty dictionary is returned in case of an error.
    :rtype: dict
    """
    try:
        from common_functions import CONFIG
        context_id = CONFIG.get("dropdown_cascading_field", {}).get("context_id")
        url = f"{JIRA_URL}/rest/api/3/field/{field_id}/context/{context_id}/option"
        all_opts = []
        start_at = 0
        while True:
            r = requests.get(url, headers=HEADERS, auth=AUTH, params={"startAt": start_at, "maxResults": 100})
            r.raise_for_status()
            data = r.json()
            all_opts.extend(data.get("values", []))
            if data.get("isLast", True): break
            start_at += 100

        tree = {}
        parents = {o["id"]: o["value"] for o in all_opts if "optionId" not in o}
        for o in all_opts:
            if "optionId" not in o:
                tree[o["value"]] = {"id": o["id"], "children": {}}
            else:
                p_name = parents.get(o["optionId"])
                if p_name: tree[p_name]["children"][o["value"]] = {"id": o["id"]}
        return tree
    except Exception as e:
        log_cascading(f"✗ Error loading options: {e}", "error"); return {}


def _update_option_id_for_service(service_name, child_id):
    """
    Updates the option ID of a business service in the AMI project based on the
    provided service name and child ID. This function queries Jira for the
    relevant issues, compares the current option ID with the provided child ID,
    and updates it if necessary.

    :param service_name: The name of the business service whose option ID needs
        to be updated.
    :param child_id: The new option ID that should be set for the business service.
    :return: None
    """
    try:
        field = CUSTOM_FIELDS.get('business_service_name')
        # Search strictly within the AMI project for Business Service items
        jql = f'project = {PROJECT_KEY} AND issuetype = "{ISSUE_TYPE}" AND "{field}" ~ "{service_name}"'

        r = requests.post(f"{JIRA_URL}/rest/api/3/search/jql", headers=HEADERS, auth=AUTH,
                          json={"jql": jql, "fields": ["key", BUSINESS_SERVICE_OPTION_ID]})
        issues = r.json().get("issues", [])

        for issue in issues:
            curr_val = issue["fields"].get(BUSINESS_SERVICE_OPTION_ID)

            # Only update if the ID is different or missing
            if str(curr_val) != str(child_id):
                if DRY_RUN:
                    log_workflow(
                        f"    🔍 [DRY RUN] AMI: Would update {issue['key']} ({service_name}) field {BUSINESS_SERVICE_OPTION_ID} to {child_id}")
                else:
                    payload = {"fields": {BUSINESS_SERVICE_OPTION_ID: str(child_id)}}
                    # Using the imported update_issue helper
                    update_issue(issue['key'], payload)
                    log_workflow(f"    ✓ Updated AMI {issue['key']} with Option ID {child_id}")
    except Exception as e:
        log_workflow(f"    ✗ Error updating {service_name}: {e}", "error")

def sync_cascading_dropdown():
    """
    Synchronizes cascading dropdown options in a Jira-like system with the current list of
    non-retired business services, updating relevant issues with the matching option IDs.

    This function ensures that the cascading dropdown field in issues reflects the correct
    relationship between business domains and services. It compares the current field
    options with the business services and updates the option IDs for services that match.

    :raises KeyError: Raised if an expected field or option is not present in the structure.

    :return: None
    """
    log_cascading("\n" + "=" * 80)
    log_cascading("=== Phase 3.4: Jira Issues Option ID Update ===")
    log_cascading("=" * 80)
    services = get_non_retired_business_services()
    options = get_cascading_field_options(CASCADING_DROPDOWN_FIELD_ID)
    if not services or not options: return

    dom_field = CUSTOM_FIELDS.get('business_domain', 'customfield_10591')
    srv_field = CUSTOM_FIELDS.get('business_service_name')

    stats = {'updated': 0}
    for issue in services:
        f = issue.get("fields", {})
        d_raw = f.get(dom_field)
        d_name = d_raw.get("value") if isinstance(d_raw, dict) else d_raw
        s_name = f.get(srv_field)
        if d_name in options and s_name in options[d_name]["children"]:
            _update_option_id_for_service(s_name, options[d_name]["children"][s_name]["id"])
            stats['updated'] += 1
    log_cascading(f"✓ Updated {stats['updated']} issues.")

def main():
    """
    Executes the main workflow for synchronizing and processing business domain data.

    The function orchestrates the workflow by resetting log files, checking for new
    business domains, loading required data, and ensuring synchronized information across
    various services and dropdown elements. It logs the workflow status and duration, and it
    handles interruptions gracefully by capturing and logging exceptions.

    :raises Exception: If an error occurs in the workflow, the function logs the error and
        re-raises it to propagate the exception.
    """
    reset_log_files()
    start_time = time.time()
    log_workflow("=== MAIN WORKFLOW START ===")
    try:
        has_new, new_ids = check_for_new_business_domains()
        if has_new:
            data = load_domain_data(DOMAIN_JSON_FILE)
            table = load_automation_rules_table_from_confluence()
            process_new_business_domains(new_ids, data, table)

        sync_cascading_dropdown_from_json_files()

        sync_business_services_from_json(SERVICE_JSON_FILE)
        sync_cascading_dropdown()

        log_workflow(f"\n🎉 COMPLETED SUCCESSFULLY in {format_duration(time.time() - start_time)}")
    except Exception as e:
        log_workflow(f"❌ INTERRUPTED: {e}", "error"); raise

if __name__ == "__main__":
    main()