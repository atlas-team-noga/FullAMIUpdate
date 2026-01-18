"""
Business Domain Automation Management Module

This module automatically manages the synchronization of Business Domains between
ServiceNow, Jira, and Confluence. It handles creation, activation, and retirement
of business domains, along with their associated automation rules.

Main Features:
-------------
1. Load business domain data from ServiceNow JSON export
2. Synchronize domain information with Confluence reference table
3. Create and manage Jira automation rules for each domain
4. Process domains based on their operational status:
   - Active (has Jira issues)
   - Planned (exists in ServiceNow but no Jira issues yet)
   - Retired (operational_status = 6 or 20)

Workflow:
---------
1. Load domain data from JSON file
2. Load existing automation table from Confluence
3. Identify Jira issues missing Business Domain field
4. Process domains with active Jira issues:
   - Execute existing automation rules
   - Create new rules if needed
5. Activate planned domains that now have issues
6. Add missing domains from ServiceNow to Confluence

Prerequisites:
--------------
- Jira API token with automation rule management permissions
- Confluence edit permissions for automation table page
- ServiceNow JSON export file with business domain data
- Configured environment variables (JIRA_URL, EMAIL, API_TOKEN)
- Template automation rule must exist in Jira

Configuration:
--------------
Required environment variables:
    - JIRA_URL: Base URL of Jira instance
    - EMAIL: Jira user email
    - API_TOKEN: Jira API token

Required config.json fields:
    - jira.project_key: Jira project key
    - jira.issue_type: Issue type for business services
    - custom_fields: Mapping of custom field IDs
    - confluence.page_id: Page ID for automation table
    - confluence.table_id: Local ID of automation table

Expected JSON Structure:
------------------------
{
    "records": [
        {
            "sys_id": "domain_sys_id",
            "name": "Domain Name",
            "u_name_fr": "Nom français",
            "u_name_nl": "Nederlandse naam",
            "operational_status": "1",  # 1=Active, 6=Retired, 20=Planned
            "u_short_domain_id": "DOM"
        }
    ]
}

    Usage Example:
    --------------
    .. code-block:: python

        from business_domain import process_business_domains

        # Process domains from ServiceNow export
        process_business_domains("servicenow_business_domain_20251103.json")

        # Expected Output:
        # === Starting Business Domain Processing ===
        # Date/Time: 2025-11-11 06:00:00
        # Loading domain data...
        # ✓ Loaded 15 domains from JSON
        # Processing domains with Jira issues...
        # ✓ 12 domains processed
        # ...

    Error Handling:
---------------
- HTTP errors are logged with full response details
- Missing domains are marked as warnings
- Failed automation rule executions are logged as errors
- All errors are written to business_domain_errors.log

Dependencies:
-------------
- requests: HTTP requests to Jira/Confluence APIs
- beautifulsoup4: Parse Confluence HTML table
- python-dotenv: Load environment variables

Notes:
------
- Automation rules are created from template rule (configured in config.json)
- Confluence table must exist before running
- Domains with name="All" are automatically ignored
- Retired domains (status 6 or 20) are tracked but not processed

See Also:
---------
- tempo3.py: Business service creation/update workflow
- create_update_business.py: Legacy business service sync

Author:
-------
Created for FPS Finance Belgium Jira automation project

Version: 1.0.0
Last Updated: 2025-11-11
"""
import requests
from bs4 import BeautifulSoup
import json
import logging
from datetime import datetime
import os
from common_functions import (
    PROJECT_KEY,
    ISSUE_TYPE,
    CUSTOM_FIELDS,
)
from config_utils import (
    JIRA_URL,
    AUTH,
    HEADERS,
    load_config_json,
    get_project_config,
    DRY_RUN
)
from logger_utils import (
    setup_logger,
    log_and_print,
    log_error_with_context
)
from jira_utils import (  # ✅ FIX: Import all needed functions from jira_utils
    fetch_jira_issues_paginated,
    make_jira_request
)
from confluence_utils import (
    get_confluence_page_content,
    find_confluence_table,
    update_confluence_page
)

# === CONFIGURATION ===
# Load configuration via utilities
config = load_config_json()
project_config = get_project_config()

# Extract configuration values
jira_url = JIRA_URL  # For compatibility with existing code
auth = AUTH
# PROJECT_KEY = project_config["project_key"]
# ISSUE_TYPE = project_config["issue_type"]
# CUSTOM_FIELDS = project_config["custom_fields"]
CONFLUENCE_PAGE_ID = project_config["confluence"]["page_id"]
CONFLUENCE_TABLE_ID = project_config["confluence"]["table_id"]
AUTO_UPDATE_CONFLUENCE = project_config["confluence"].get("auto_update_table", False)

# === LOGGING SETUP ===
# Use centralized loggers with specific names
main_logger = setup_logger('business_domain', 'business_domain_automation.log', clear_on_start=True)
error_logger = setup_logger('business_domain_errors', 'business_domain_errors.log', logging.ERROR, clear_on_start=True)

print(f"✓ Business Domain module loaded - connecting to: {jira_url}")

# Global variable for mapping
BUSINESS_DOMAIN_MAPPING = {}
# Cache to avoid multiple calls
_DOMAIN_CACHE = None
_CACHE_TIMESTAMP = None
CACHE_DURATION = 300


# === JIRA API FUNCTIONS ===
def search_issues_missing_business_domain():
    """
    Searches for issues in a specific project and issue type where either the 'Business domain id' or
    'Business service id' fields are empty. Excludes a predefined set of fictional issues.

    The function sends requests to a JIRA API endpoint in batches and retrieves all matching issues
    based on a specified JQL query. The results include specified custom fields for each issue.

    :return: A list of issues with missing 'Business domain id' or 'Business service id' fields,
             or an empty list in case of errors.
    :rtype: list
    :raises requests.exceptions.HTTPError: On HTTP errors during API calls.
    :raises Exception: For any other unexpected errors.
    """
    try:
        # Exclude fictional Business Services
        jql = (f'project = {PROJECT_KEY} AND '
               f'issuetype = "{ISSUE_TYPE}" AND '
               f'("Business domain id[Short text]" IS EMPTY OR "Business service id[Short text]" is EMPTY) AND '
               f'key NOT IN (AMI-4925, AMI-4926, AMI-4927)')

        url = f"{jira_url}/rest/api/3/search/jql"

        all_issues = []
        start_at = 0
        max_results = 100

        while True:
            params = {
                "jql": jql,
                "startAt": start_at,
                "maxResults": max_results,
                "fields": f"summary,{CUSTOM_FIELDS['business_domain_id']},{CUSTOM_FIELDS['business_domain']}"
            }

            response = requests.get(url, headers=HEADERS, auth=auth, params=params)
            response.raise_for_status()

            result = response.json()
            issues = result.get("issues", [])
            all_issues.extend(issues)

            total = result.get("total", 0)
            if start_at + max_results >= total or len(issues) == 0:
                break

            start_at += max_results

        log_and_print(f"Found {len(all_issues)} issues with Business domain id but without Business domain", "info")
        return all_issues

    except requests.exceptions.HTTPError as err:
        log_error_with_context("search_issues", f"HTTP Error: {err}",
                               {"response": err.response.text if err.response else None})
        return []
    except Exception as err:
        log_error_with_context("search_issues", str(err), {"exception_type": type(err).__name__})
        return []


# def get_business_domain_mapping_from_table(automation_table):
#     """
#     Extracts a mapping dictionary between business_domain_id and business_domain_name
#     from the Confluence automation rules table.
#
#     This function converts the automation table structure into a simple ID->Name mapping
#     that can be used by other modules.
#
#     :param automation_table: Dictionary from load_automation_rules_table_from_confluence()
#     :type automation_table: dict
#     :return: Dictionary mapping business_domain_id to business_domain_name
#     :rtype: dict
#     """
#     domain_mapping = {}
#
#     for sys_id, domain_data in automation_table.items():
#         domain_name = domain_data.get("domain_name", "").strip('"').replace('\\/', '/')
#         # Remove markers (planned), (retired), etc.
#         domain_name = domain_name.replace(" (planned)", "").replace("(planned)", "").strip()
#         # Clean improperly closed or double quotes
#         domain_name = domain_name.replace('""', '"').strip('"').strip()
#         domain_mapping[sys_id] = domain_name
#
#     return domain_mapping

def get_unique_business_domain_ids(issues):
    """
    Extracts and returns a set of unique business domain IDs from a list of issue dictionaries.

    Each issue in the list is expected to contain nested dictionary data under the "fields"
    key, from which the business domain ID is retrieved based on a predefined custom field.
    If the business domain ID exists, it is added to the resulting set.

    :param issues: A list of dictionaries, where each dictionary represents an issue
        and contains nested data. The business domain ID is retrieved from the "fields"
        key using a predefined custom key mapping.
    :type issues: list[dict]

    :return: A set containing the unique business domain IDs extracted from the input issues.
    :rtype: set
    """
    domain_ids = set()

    for issue in issues:
        domain_id = issue.get("fields", {}).get(CUSTOM_FIELDS["business_domain_id"])
        if domain_id:
            domain_ids.add(domain_id)

    return domain_ids


# === CONFLUENCE API FUNCTIONS ===


def load_automation_rules_table_from_confluence(force_refresh=False):
    """
    Loads the automation rules table from a Confluence page and returns the associated
    business domain mapping. The function fetches data from Confluence, processes the
    table content to extract relevant information, and caches the results to optimize
    subsequent calls. The cache can be bypassed with a force refresh.

    :param force_refresh: If True, bypasses the cache and fetches data directly from
                          Confluence. Defaults to False.
    :type force_refresh: bool

    :return: A dictionary mapping system IDs to their corresponding business domain
             information, including domain name and data source. Returns an empty
             dictionary if the operation fails or insufficient data is available.
    :rtype: dict
    """
    global _DOMAIN_CACHE, _CACHE_TIMESTAMP

    # ✅ Check if cache can be used
    if not force_refresh and _DOMAIN_CACHE is not None and _CACHE_TIMESTAMP is not None:
        import time
        cache_age = time.time() - _CACHE_TIMESTAMP
        if cache_age < CACHE_DURATION:
            log_and_print(
                f"  → Using cached domain mapping (age: {cache_age:.1f}s)",
                "debug",
                main_logger
            )
            return _DOMAIN_CACHE

    try:
        import html
        import time

        log_and_print("Loading Business Domain mapping from Confluence...", "info", main_logger)

        # ✅ Use utility functions
        page_data, current_version, html_content, soup = get_confluence_page_content(CONFLUENCE_PAGE_ID)

        if not soup:
            log_and_print("  ✗ Could not fetch Confluence page", "error", error_logger)
            return {}

        table = find_confluence_table(soup, CONFLUENCE_TABLE_ID)

        if not table:
            log_and_print("  ✗ Table not found on Confluence page", "error", error_logger)
            return {}

        rows = table.find_all('tr')

        if len(rows) < 2:
            log_and_print("  ✗ Table has insufficient rows (less than 2)", "warning", main_logger)
            return {}

        headers = [ele.get_text(strip=True) for ele in rows[0].find_all(['td', 'th'])]
        log_and_print(f"  → Table headers: {headers}", "debug", main_logger)
        log_and_print(f"  → Total rows in table (including header): {len(rows)}", "info", main_logger)

        domain_mapping = {}
        skipped_rows = 0

        for idx, row in enumerate(rows[1:], start=1):
            cols = row.find_all(['td', 'th'])
            cols_text = [ele.get_text(strip=True) for ele in cols]

            log_and_print(f"  → Row {idx}: {len(cols)} columns - {cols_text[:2]}", "debug", main_logger)

            if len(cols_text) < 2:
                skipped_rows += 1
                log_and_print(f"  ⚠ Row {idx}: Skipped - insufficient columns ({len(cols_text)})", "info", main_logger)
                continue

            sys_id = cols_text[0].strip()
            domain_name_raw = cols_text[1]

            domain_name = html.unescape(domain_name_raw)
            domain_name = domain_name.strip('"').replace('\\/', '/')

            domain_name_clean = domain_name.replace(" (planned)", "").replace("(planned)", "").strip()
            domain_name_clean = domain_name_clean.replace(" (Planned)", "").replace("(Planned)", "").strip()
            domain_name_clean = domain_name_clean.replace(" (retired)", "").replace("(retired)", "").strip()
            domain_name_clean = domain_name_clean.replace(" (Retired)", "").replace("(Retired)", "").strip()

            domain_name_clean = domain_name_clean.replace('""', '"').strip('"').strip()

            if not sys_id or not domain_name_clean:
                skipped_rows += 1
                log_and_print(
                    f"  ⚠ Row {idx}: Skipped - empty (sys_id='{sys_id}', domain='{domain_name_clean}')",
                    "info",
                    main_logger
                )
                continue

            domain_mapping[sys_id] = {
                "domain_name": domain_name_clean,
                "source": "confluence"
            }

            log_and_print(f"  ✓ Row {idx}: Loaded {sys_id} = {domain_name_clean}", "info", main_logger)

        log_and_print(f"  → Skipped {skipped_rows} rows (empty or invalid)", "info", main_logger)
        log_and_print(f"✓ Loaded {len(domain_mapping)} business domains from Confluence", "info", main_logger)

        # ✅ Save to cache
        _DOMAIN_CACHE = domain_mapping
        _CACHE_TIMESTAMP = time.time()

        return domain_mapping

    except Exception as err:
        log_error_with_context("load_confluence_table", str(err), {
            "exception_type": type(err).__name__
        }, error_logger)
        return {}


def update_confluence_table_with_new_domain(domain_info, is_planned=False, rule_id=None):
    """
    Updates a Confluence table with a new business domain and its associated details. This function fetches the current
    page content, processes the table to find a matching template row, and appends a new row with updated information
    about the domain. Optionally, an automation rule link can be added if `rule_id` is provided.

    :param domain_info: Dictionary containing domain details.
    :param is_planned: Boolean flag indicating if the domain is planned (default: False).
    :param rule_id: Optional string representing the automation rule ID (default: None).
    :return: Boolean indicating if the operation succeeded (True) or failed (False).
    """
    try:
        # Extract domain name: ONLY 'name'
        domain_name = domain_info.get('name') or 'Unknown'
        domain_sys_id = domain_info.get('sys_id')

        log_and_print(f"  → Adding {domain_name} to the Confluence table...", "info")
        log_and_print(f"  → Domain sys_id: {domain_sys_id}", "debug")

        # Step 1: Get current page content using utility function
        log_and_print(f"  → Fetching Confluence page: {CONFLUENCE_PAGE_ID}", "debug")
        page_data, current_version, html_content, soup = get_confluence_page_content(CONFLUENCE_PAGE_ID)
        if not page_data:
            return False

        log_and_print(f"  → Current page version: {current_version}", "debug")

        # Step 2: Parse HTML and find the table using utility function
        table = find_confluence_table(soup, CONFLUENCE_TABLE_ID)
        if not table:
            return False

        log_and_print(f"  → Table found with ID: {CONFLUENCE_TABLE_ID}", "debug")

        # Step 3: Extract HTML templates from existing row
        tbody = table.find('tbody') or table
        existing_rows = tbody.find_all('tr')

        # Get the first data row (skip header) to use as template
        template_row = None
        if len(existing_rows) > 1:
            template_row = existing_rows[1]

        if not template_row:
            log_and_print(f"  ✗ No template row found to copy format", "error")
            return False

        # Extract raw HTML from template cells
        template_cells = template_row.find_all('td')

        if len(template_cells) < 3:
            log_and_print(f"  ✗ Template row has insufficient cells", "error")
            return False

        # Get inner HTML of each template cell (preserve all attributes and structure)
        def extract_cell_template(cell):
            """Extract cell HTML and prepare it for value replacement"""
            cell_html = str(cell)
            # Get just the content between <td...> and </td>
            # Find the text content to replace
            text_content = cell.get_text(strip=True)
            return cell_html, text_content

        td1_template, td1_text = extract_cell_template(template_cells[0])
        td2_template, td2_text = extract_cell_template(template_cells[1])
        td3_template, td3_text = extract_cell_template(template_cells[2])

        log_and_print(f"  → Template cell 1 text: '{td1_text}'", "debug")
        log_and_print(f"  → Template cell 2 text: '{td2_text}'", "debug")

        # Step 4: Create new cells by replacing template content

        # Column 1: sys_id (replace the text in template)
        td1_new = td1_template.replace(td1_text, domain_sys_id)

        # Column 2: Domain name with quotes and (planned) if needed
        if is_planned:
            domain_display = f'"{domain_name}" (planned)'
        else:
            domain_display = f'"{domain_name}"'
        td2_new = td2_template.replace(td2_text, domain_display)

        # Column 3: Schedule
        schedule_text = "Monthly on day 1 at 6:00 AM"
        td3_new = td3_template.replace(td3_text, schedule_text)

        # Column 4: Automation rule (create from scratch as it needs a link)
        # Copy attributes from template cell
        td4_attrs = ""
        if len(template_cells) > 3:
            for attr_name, attr_value in template_cells[3].attrs.items():
                if isinstance(attr_value, list):
                    attr_value = ' '.join(attr_value)
                td4_attrs += f' {attr_name}="{attr_value}"'

        if rule_id:
            rule_url = f"{jira_url}/jira/settings/automation#/rule/{rule_id}"
            rule_link_text = f"Update Business Domain field = {domain_name}"
            td4_content = f'<a href="{rule_url}">{rule_link_text}</a>'
            if is_planned:
                td4_content += '<br/> (⏸️ DISABLED)'
        else:
            if is_planned:
                td4_content = "⏸️ Planned (no automation needed yet)"
            else:
                td4_content = "⚠️ TODO: Create automation rule"

        td4_new = f'<td{td4_attrs}>{td4_content}</td>'

        # Step 5: Assemble the new row
        new_row_html = f"<tr>{td1_new}{td2_new}{td3_new}{td4_new}</tr>"

        log_and_print(f"  → New row HTML (first 400 chars): {new_row_html[:400]}", "debug")

        # Step 6: Insert the new row into tbody
        tbody_html = str(tbody)

        # Find the closing </tbody> tag and insert before it
        if '</tbody>' in tbody_html:
            tbody_html = tbody_html.replace('</tbody>', f'{new_row_html}</tbody>')
        else:
            # If no </tbody>, append before end of table
            tbody_html = tbody_html.rstrip('</table>').rstrip() + new_row_html + '</table>'

        # Replace the old tbody with the new one
        new_tbody = BeautifulSoup(tbody_html, 'html.parser')
        tbody.replace_with(new_tbody.find('tbody') or new_tbody)

        log_and_print(f"  → Row inserted using HTML template preservation method", "debug")

        # Step 7: Update the page using utility function
        updated_html = str(soup)
        status_msg = " (planned - no Jira issues yet)" if is_planned else ""
        commit_message = f"Added Business Domain: {domain_name}{status_msg} (sys_id: {domain_sys_id})"

        log_and_print(f"  → Sending PUT request to update page (version {current_version} → {current_version + 1})",
                      "debug")

        if DRY_RUN:
            log_and_print(f"  🔍 [DRY RUN] Would add {domain_name} to Confluence table (sys_id: {domain_sys_id})",
                          "info")
            if rule_id:
                log_and_print(f"  🔍 [DRY RUN] Would add automation rule link: {rule_id}", "info")
            return True

        if update_confluence_page(CONFLUENCE_PAGE_ID, page_data, current_version, updated_html, commit_message):
            log_and_print(f"  ✓ Successfully added {domain_name} to Confluence table", "info")
            if rule_id:
                log_and_print(f"  ✓ Automation rule link added: {rule_id}", "info")
            return True
        else:
            log_and_print(f"  ✗ Failed to update Confluence page", "error")
            return False

    except Exception as err:
        log_error_with_context(
            "update_confluence_table",
            str(err),
            {"domain_info": domain_info, "rule_id": rule_id, "exception_type": type(err).__name__}
        )
        return False


def get_business_domain_info_from_json(domain_id, domains_data):
    """
    Extracts business domain information from a JSON-like structure based on the provided domain ID.

    This function parses a given JSON-like structure, which may be either a dictionary or a list, to
    find a specific business domain identified by a unique domain ID. The function returns a dictionary
    containing the domain's system identifier (`sys_id`) and name if a matching domain is found. If there
    is no match or if the input structure is invalid, the function returns `None`.

    :param domain_id: A unique identifier for the desired business domain.
    :type domain_id: str

    :param domains_data: A dictionary or list containing business domain records. If a dictionary is provided,
        it must contain a key 'records', which holds a list of domain entries. Each domain entry should
        be a dictionary containing the 'sys_id' key.
    :type domains_data: Union[Dict, List]

    :return: A dictionary with keys 'sys_id' and 'name' if a matching domain is found. Returns `None` if
        no domain matches or if the input is invalid.
    :rtype: Optional[Dict[str, str]]
    """
    if isinstance(domains_data, dict) and "records" in domains_data:
        domain_list = domains_data["records"]
    elif isinstance(domains_data, list):
        domain_list = domains_data
    else:
        return None

    for domain in domain_list:
        if isinstance(domain, dict) and domain.get("sys_id") == domain_id:
            return {
                "sys_id": domain.get("sys_id"),
                "name": domain.get("name", "")
            }

    return None


def fill_business_domain_field_direct(domain_sys_id, domain_name):
    """
    Fills the "Business Domain" field for Jira issues that match the given domain system ID and are missing the
    Business Domain name. This function searches for issues based on the specified project and issue type,
    constructs a textual value to update the field, and applies the update directly.

    :param domain_sys_id: The identifier of the business domain to match in Jira issues.
    :type domain_sys_id: str
    :param domain_name: The textual name of the business domain used to update the issues.
    :type domain_name: str
    :return: A tuple containing the count of successfully updated issues (int) and the count of errors (int).
    :rtype: tuple
    """
    try:
        log_and_print(f"\n→ Filling Business Domain field for: {domain_name} (sys_id: {domain_sys_id})", "info",
                      main_logger)

        # ⚠ IMPORTANT : customfield_12658 does NOT support Jira options.
        # We write the text value directly into the field instead of
        # trying to create an option via add_select_field_option.

        # STEP 1: Search for issues with this domain_sys_id but missing Business Domain name
        jql = (f'project = {PROJECT_KEY} AND '
               f'issuetype = "{ISSUE_TYPE}" AND '
               f'"{CUSTOM_FIELDS["business_domain_id"]}" ~ {domain_sys_id} AND '
               f'"{CUSTOM_FIELDS["business_domain"]}" is EMPTY')
        print("JQL in business_domain.py line 620", jql)

        # ✅ FIX: Use fetch_jira_issues_paginated correctly (only accepts jql parameter)
        issues = fetch_jira_issues_paginated(jql)

        total = len(issues)

        if total == 0:
            log_and_print(f"  ✓ No issues to update (all already have Business Domain set)", "info", main_logger)
            return 0, 0

        log_and_print(f"  → Found {total} issues to update", "info", main_logger)

        success_count = 0
        error_count = 0

        # STEP 2: Update each issue
        for issue in issues:
            issue_key = issue.get("key")

            try:
                # Here we set the TEXTUAL value directly.
                payload = {
                    "fields": {
                        CUSTOM_FIELDS["business_domain"]: domain_name
                    }
                }

                log_and_print(f"  → Updating {issue_key} with payload: {json.dumps(payload)}", "debug", main_logger)

                if DRY_RUN:
                    log_and_print(f"  🔍 [DRY RUN] Would update {issue_key} with Business Domain: {domain_name}", "info",
                                  main_logger)
                    success_count += 1
                else:
                    endpoint = f"/rest/api/3/issue/{issue_key}"
                    result = make_jira_request(
                        "PUT",
                        endpoint,
                        data=payload,
                        log_function=lambda msg, level="info": log_and_print(msg, level, main_logger)
                    )

                    if result is not None:
                        success_count += 1
                        log_and_print(f"  ✓ Updated {issue_key}", "info", main_logger)
                    else:
                        error_count += 1
                        log_and_print(f"  ✗ Failed to update {issue_key} (check logs above for details)", "error",
                                      error_logger)

            except Exception as err:
                error_count += 1
                log_and_print(f"  ✗ Failed to update {issue_key}: {err}", "error", error_logger)

        log_and_print(f"  📊 Results: {success_count} updated, {error_count} errors", "info", main_logger)
        return success_count, error_count

    except Exception as err:
        log_error_with_context(
            "fill_business_domain_field",
            str(err),
            {"domain_sys_id": domain_sys_id, "domain_name": domain_name},
            error_logger
        )
        return 0, 0


# === MAIN WORKFLOW ===

def process_business_domains(domain_json_file_process):
    """
    Process business domains: detect new domains, activate planned domains, and ensure all domains are in Confluence.

    :param domain_json_file_process: Path to the JSON file containing business domain data
    :return: None
    """
    log_and_print("=== Starting Business Domain Automation Management ===", "info")
    log_and_print(f"Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")

    # Load domain data
    domains_data = _load_domain_data(domain_json_file_process)
    if not domains_data:
        return

    # Load Confluence table
    automation_table = load_automation_rules_table_from_confluence()

    # Get issues missing business domain
    issues = search_issues_missing_business_domain()
    unique_domain_ids = get_unique_business_domain_ids(issues) if issues else set()

    # Initialize counters
    stats = {
        'executed': 0,
        'not_found': 0,
        'activated': 0,
        'planned_added': 0,
        'retired_added': 0,
        'errors': 0
    }

    # Step 1: Process domains with Jira issues
    if unique_domain_ids:
        log_and_print(f"\n--- Step 1: Processing {len(unique_domain_ids)} domains with Jira issues ---", "info")
        _process_domains_with_issues(list(unique_domain_ids), automation_table, domains_data, stats)

    # Step 2: Activate planned domains that now have issues
    if unique_domain_ids:
        log_and_print("\n--- Step 2: Activating planned domains ---", "info")
        _activate_planned_domains(list(unique_domain_ids), automation_table, domains_data, stats)

    # Step 3: Add all missing domains from ServiceNow to Confluence
    log_and_print("\n--- Step 3: Ensuring ALL domains from ServiceNow are in Confluence ---", "info")
    _add_missing_domains_to_confluence(domains_data, automation_table, unique_domain_ids, stats)

    # Final summary
    _print_summary(unique_domain_ids, stats)


def _load_domain_data(json_file):
    """
    Loads domain data from a specified JSON file.

    This function attempts to open and read a given JSON file.
    If the file does not exist, or an error occurs during the
    reading process, the function handles the failure by logging
    an appropriate error message. On success, the function
    returns the parsed JSON data as a Python object.

    :param json_file: Path to the JSON file to be loaded.
    :type json_file: str
    :return: The parsed JSON data if successful, otherwise None.
    :rtype: dict or list or None
    """
    if not os.path.exists(json_file):
        log_and_print(f"Error: file {json_file} not found", "error")
        return None

    try:
        with open(json_file, 'r', encoding='utf-8') as file_json:
            return json.load(file_json)
    except Exception as err:
        log_error_with_context("load_json", f"JSON reading error: {err}", {})
        return None


def _process_domains_with_issues(domain_ids, automation_table, domains_data, stats):
    """
    Processes domains with reported issues by handling both existing and newly detected
    domains based on their presence in the provided automation table.

    :param domain_ids: A list of domain identifiers requiring processing.
    :type domain_ids: list
    :param automation_table: A mapping of existing domains containing automation-related
        properties and data.
    :type automation_table: dict
    :param domains_data: A collection of data attributes associated with each domain.
    :type domains_data: dict
    :param stats: A statistics object or dictionary used for maintaining tracking
        information during processing.
    :type stats: dict
    :return: None
    """
    for domain_id in domain_ids:
        log_and_print(f"\n→ Processing domain: {domain_id}", "info")

        if domain_id in automation_table:
            # Domain exists in table
            _handle_existing_domain(domain_id, automation_table, stats)
        else:
            # New domain detected
            _handle_new_domain(domain_id, domains_data, stats)


def _handle_existing_domain(domain_id, automation_table, stats):
    """
    Handles an existing domain by processing automation table rules and updating the
    related business domain field directly. Updates statistics to reflect the number
    of successful changes made based on the domain information.

    :param domain_id: Unique identifier of the domain being processed.
    :type domain_id: str
    :param automation_table: A mapping of domain IDs to their corresponding automation
        rule information.
    :type automation_table: dict
    :param stats: A dictionary tracking execution statistics such as the number of
        updates performed for business domain fields.
    :type stats: dict
    :return: None
    """
    rule_info = automation_table[domain_id]
    domain_name = rule_info.get("domain_name")

    log_and_print(f"  ✓ Domain found: {domain_name}", "info")

    # Fill Business Domain field directly instead of using automation rules
    success, errors = fill_business_domain_field_direct(domain_id, domain_name)

    if success > 0:
        stats['executed'] += 1
        log_and_print(f"  ✓ {success} issues updated with Business Domain field", "info")
    else:
        log_and_print(f"  → No updates needed for {domain_name}", "info")


def _handle_new_domain(domain_id, domains_data, stats):
    """
    Processes a newly detected domain and updates relevant systems and statistics.

    This function handles new business domains by retrieving their details, logging relevant
    information, and performing updates to systems such as issue trackers and Confluence. It
    tracks both successful updates and failures, and requires manual action if automatic updates
    to Confluence are disabled.

    :param domain_id: The unique identifier of the domain to be processed.
    :type domain_id: str
    :param domains_data: A collection of data containing information about multiple domains.
    :type domains_data: dict
    :param stats: A dictionary tracking statistics of operations such as the number of executed
                  updates, errors encountered, and domains not found.
    :type stats: dict
    :return: None
    """
    domain_info = get_business_domain_info_from_json(domain_id, domains_data)

    if not domain_info:
        log_and_print(f"  ✗ Domain {domain_id} not found in JSON", "error")
        stats['not_found'] += 1
        return

    # ONLY 'name'
    domain_name = domain_info.get("name") or "Unknown"
    log_and_print(f"  ⚠ NEW domain: {domain_name}", "warning")

    # Fill Business Domain field directly
    success, errors = fill_business_domain_field_direct(domain_id, domain_name)

    if success > 0:
        log_and_print(f"  ✓ {success} issues updated with Business Domain field", "info")
        stats['executed'] += 1

    if not AUTO_UPDATE_CONFLUENCE:
        log_and_print(f"  → ACTION REQUIRED: Manually add {domain_name} to Confluence", "warning")
        stats['not_found'] += 1
        return

    # Add to Confluence (without automation rule)
    if update_confluence_table_with_new_domain(domain_info, is_planned=False, rule_id=None):
        log_and_print(f"  ✓ Added to Confluence table", "info")
    else:
        log_and_print(f"  ✗ Failed to update Confluence", "error")
        stats['errors'] += 1


def _activate_planned_domains(domain_ids, automation_table, domains_data, stats):
    """
    Activates planned domains by processing domain entries from the automation table,
    ensuring certain conditions are met, and directly updating relevant fields. The
    function increments activation statistics for successfully activated domains.

    :param domain_ids: List of identifiers representing domains to be processed.
    :type domain_ids: list[str]
    :param automation_table: Dictionary containing domain data with domain IDs as keys.
    :type automation_table: dict
    :param domains_data: Additional data source with information about the domains.
    :type domains_data: dict
    :param stats: Dictionary to track activation statistics, including the number of
        successfully activated domains.
    :type stats: dict
    :return: None
    """
    for domain_id in domain_ids:
        if domain_id not in automation_table:
            continue

        domain_entry = automation_table[domain_id]
        domain_name = domain_entry.get("domain_name", "")

        if "(planned)" not in domain_name.lower():
            continue

        # Get operational status
        operational_status = _get_operational_status(domain_id, domains_data)

        if operational_status not in ["1", "", None]:
            continue

        # Activate the domain by filling the fields directly
        clean_name = domain_name.replace(" (planned)", "").replace("(planned)", "").strip('"')

        log_and_print(f"\n→ Activating planned domain: {clean_name}", "info")

        # Fill Business Domain field directly
        success, errors = fill_business_domain_field_direct(domain_id, clean_name)

        if success > 0:
            stats['activated'] += 1
            log_and_print(f"  ✓ {success} issues updated for {clean_name}", "info")


def _add_missing_domains_to_confluence(domains_data, automation_table, processed_ids, stats):
    """
    Adds missing domains to Confluence based on their operational status.

    This function processes a list of domain information to identify and add domains to
    Confluence that are not already in the automation table or the list of processed IDs.
    Domains are categorized and processed based on their operational status, which can
    be "Planned", "Retired", or "Active". It updates the provided statistics dictionary
    accordingly during the process.

    :param domains_data: A dictionary or list containing domain records. If it is a dictionary,
        the function expects a key "records" containing a list of domain dictionaries.
    :param automation_table: A set of sys_id values representing already processed domains
        in the automation system.
    :param processed_ids: A set of sys_id values representing domains that have been handled
        during a previous process.
    :param stats: A dictionary used to track statistics during the function execution for
        different categories of domains.

    :return: None
    """
    all_domains = domains_data.get("records", []) if isinstance(domains_data, dict) else domains_data

    for domain in all_domains:
        if not isinstance(domain, dict):
            continue

        domain_sys_id = domain.get("sys_id")

        # Skip if already processed
        if domain_sys_id in automation_table or domain_sys_id in processed_ids:
            continue

        operational_status = domain.get("operational_status")
        domain_name = domain.get("name")

        if not domain_name:
            continue

        # We can still keep other fields for info, but they will not be used for the name
        domain_info = {
            "sys_id": domain_sys_id,
            "name": domain.get("name", "")
        }

        # Process based on status
        if operational_status == "20":  # Planned
            _add_planned_domain(domain_info, stats)
        elif operational_status == "6":  # Retired
            _add_retired_domain(domain_info, stats)
        elif operational_status in ["1", "", None]:  # Active
            _add_active_domain(domain_info, stats)  # Active
            _add_active_domain(domain_info, stats)


def _add_planned_domain(domain_info, stats):
    """
    Adds a planned domain into the tracking system and updates relevant statistics.

    This function is responsible for handling the addition of a planned domain when
    automatic updates to Confluence are enabled. It retrieves the domain name from
    the provided domain information, logs the operation, and updates the Confluence
    table associated with planned domains. If the operation succeeds, the planned
    domain addition count is incremented in the given stats.

    :param domain_info: Dictionary containing details about the domain to be added.
                       Expected to include at least the 'name' key.
    :type domain_info: dict
    :param stats: Dictionary holding statistical counters for domain additions.
                  Should include the 'planned_added' key for proper functionality.
    :type stats: dict
    :return: None
    :rtype: None
    """
    if not AUTO_UPDATE_CONFLUENCE:
        return

    domain_name = domain_info.get('name')
    log_and_print(f"\n→ Adding planned domain: {domain_name}", "info")

    if update_confluence_table_with_new_domain(domain_info, is_planned=True, rule_id=None):
        stats['planned_added'] += 1


def _add_retired_domain(domain_info, stats):
    """
    Adds a retired domain to a confluence table and updates statistics if auto-update is enabled.

    This function checks whether the auto-update feature is enabled (`AUTO_UPDATE_CONFLUENCE`)
    and proceeds to add a retired domain's information to the confluence table. It logs the
    attempt and, if successful, increments the counter for retired domains added in the `stats`.

    :param domain_info: Dictionary containing information about the domain to be added.
    :type domain_info: dict
    :param stats: Dictionary holding statistical counters, including the count for retired
                  domains added.
    :type stats: dict
    :return: None
    """
    if not AUTO_UPDATE_CONFLUENCE:
        return

    domain_name = domain_info.get('name')
    log_and_print(f"\n→ Adding retired domain: {domain_name}", "info")

    if update_confluence_table_with_new_domain(domain_info, is_planned=True, rule_id=None):
        stats['retired_added'] += 1


def _add_active_domain(domain_info, stats):
    """
    Adds an active domain to the system if auto-update is enabled.

    This function checks if the `AUTO_UPDATE_CONFLUENCE` flag is enabled and attempts
    to add a new active domain to the Confluence table. It logs the progress and
    updates the statistics upon successful addition.

    :param domain_info: A dictionary containing details of the domain.
    :param stats: A dictionary tracking statistics such as planned additions.
    :return: None
    """
    if not AUTO_UPDATE_CONFLUENCE:
        return

    domain_name = domain_info.get('name')
    log_and_print(f"\n→ Adding active domain (no issues yet): {domain_name}", "info")

    if update_confluence_table_with_new_domain(domain_info, is_planned=True, rule_id=None):
        stats['planned_added'] += 1


def _get_operational_status(domain_id, domains_data):
    """
    Determines the operational status of a specified domain based on the given
    domain ID and data. The function iterates through a provided list or dictionary
    of domain records to find the match and extract the corresponding status
    attribute.

    :param domain_id: Unique identifier of the domain to locate
        operational status for.
    :type domain_id: str
    :param domains_data: Collection of domain records, either as a list of
        dictionaries or as a dictionary containing a "records" key, which is a list
        of domain dictionaries.
    :type domains_data: Union[dict, list]
    :return: The operational status of the matched domain if successfully located;
        otherwise, None if no matching record is found.
    :rtype: Optional[str]
    """
    domain_list = domains_data.get("records", []) if isinstance(domains_data, dict) else domains_data

    for domain in domain_list:
        if isinstance(domain, dict) and domain.get("sys_id") == domain_id:
            return domain.get("operational_status")

    return None


def _print_summary(domain_ids, stats):
    """
    Logs and prints a summary of the processing execution details for a set of domains.
    This function outputs counts of various domain statuses and operation metrics.

    :param domain_ids:
        A list of domain identifiers that were processed. Each identifier should
        denote a specific domain associated with Jira issues.
    :param stats:
        A dictionary containing statistical metrics related to processing results.
        The expected keys in this dictionary are:

        - 'executed': Number of domains with existing rules.
        - 'not_found': Number of new domains that were processed but not found in prior records.
        - 'activated': Number of planned domains that were successfully activated.
        - 'planned_added': Total number of planned/active domains added.
        - 'retired_added': Total count of retired domains that were added.
        - 'errors': Number of errors encountered during the operation.

    :return:
        None. The function performs logging and printing operations but does not
        return any values.
    """
    log_and_print("\n=== Processing Summary ===", "info")
    log_and_print(f"Domains with Jira issues: {len(domain_ids)}", "info")
    log_and_print(f"✓ Domains with existing rules: {stats['executed']}", "info")
    log_and_print(f"⚠ New domains processed: {stats['not_found']}", "info")
    log_and_print(f"🔄 Planned domains activated: {stats['activated']}", "info")
    log_and_print(f"📝 Planned/Active domains added: {stats['planned_added']}", "info")
    log_and_print(f"🗃️ Retired domains added: {stats['retired_added']}", "info")
    log_and_print(f"✗ Errors: {stats['errors']}", "info")
    log_and_print("\n=== Process completed ===", "info")

