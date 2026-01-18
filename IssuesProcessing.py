"""
Business Service Synchronization Module - OPTIMIZED VERSION

✅ OPTIMIZATION: Single-pass processing
- Load all Business Services once into memory
- Process labels and updates in the same iteration
- Drastically reduces API calls from ~600 to ~2
"""

import json
import os
import time
from datetime import datetime
from typing import Dict, Tuple, Optional

from config_utils import DRY_RUN, JIRA_URL, AUTH, HEADERS
from common_functions import (
    PROJECT_KEY,
    ISSUE_TYPE,
    CUSTOM_FIELDS,
    should_exclude_item
)
from logger_utils import log_and_print, log_error_with_context, format_duration
from jira_utils import (
    make_jira_request,
    update_issue,
    search_issue_by_sys_id
)
from Business_service import (
    map_operational_status,
    get_account_id_by_email
)
from rate_limit_config import LABEL_REMOVAL_DELAY

# === CONFIGURATION ===
VERIFICATION_LABEL = "business-service-verified"
json_file = "servicenow_cmdb_ci_service_20251103_133624.json"

# ✅ Cache for mandatory reporter to avoid repeated API calls in loops
MANDATORY_REPORTER_ID = None

def get_mandatory_reporter_id():
    """
    Retrieves the mandatory reporter ID from a global variable. If the global variable
    is not yet set, it resolves the ID using a predefined email address and a function
    to fetch the account ID. Logs the resolution process upon successfully retrieving
    the ID.

    :return: The mandatory reporter ID.
    :rtype: str or None
    """
    global MANDATORY_REPORTER_ID
    if MANDATORY_REPORTER_ID is None:
        mandatory_email = "atlassian.actions@minfin.fed.be"
        MANDATORY_REPORTER_ID = get_account_id_by_email(mandatory_email)
        if MANDATORY_REPORTER_ID:
            log_and_print(f"✅ Mandatory Reporter ID resolved: {MANDATORY_REPORTER_ID}", "info")
    return MANDATORY_REPORTER_ID

def load_all_business_services_with_fields() -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """
    ✅ OPTIMIZATION: Load ALL Business Services at once
    with ALL necessary fields.

    Returns TWO dictionaries: one indexed by sys_id, one by normalized name.

    :return: Tuple (indexed_by_sys_id, indexed_by_name)
    """
    start_time = time.time()
    log_and_print("\n🔄 Loading all Business Services into cache...", "info")

    try:
        import requests

        jql = f'project = {PROJECT_KEY} AND issuetype = "{ISSUE_TYPE}" AND key NOT IN (AMI-4925, AMI-4926, AMI-4927)'

        # ✅ FIX: Use /search/jql endpoint with POST instead of GET /search
        url = f"{JIRA_URL}/rest/api/3/search/jql"

        all_issues = []
        next_page_token = None
        max_results = 100

        while True:
            # ✅ FIX: Use POST with nextPageToken for pagination
            payload = {
                "jql": jql,
                "maxResults": max_results,
                "fields": ["*all"],  # ✅ FIX: Array instead of string
                "fieldsByKeys": True
            }

            # Add nextPageToken if present
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            response = requests.post(
                url,
                headers=HEADERS,
                auth=AUTH,
                data=json.dumps(payload)
            )
            response.raise_for_status()

            result = response.json()
            issues = result.get("issues", [])
            all_issues.extend(issues)

            total = result.get("total", 0)
            log_and_print(
                f"  → Progress: {len(all_issues)}/{total} issues loaded...",
                "debug"
            )

            # Get nextPageToken for next page
            next_page_token = result.get("nextPageToken")

            # If nextPageToken is null, it's the last page
            if not next_page_token:
                break

            # Safety: if no issues returned, stop
            if len(issues) == 0:
                break

        # ✅ Index by sys_id for O(1) quick lookup
        indexed_issues = {}
        name_indexed_issues = {}  # ✅ Secondary index for name matching
        business_service_id_field = CUSTOM_FIELDS.get("business_service_id")

        for issue in all_issues:
            fields = issue.get("fields", {})
            sys_id = fields.get(business_service_id_field)
            summary = fields.get("summary", "")

            if sys_id:
                indexed_issues[sys_id] = issue

            # ✅ Build name index by stripping "Retired-" prefixes
            clean_name = summary.split(" | ")[0]
            for prefix in ["Retired-", "Retired - "]:
                if clean_name.startswith(prefix):
                    clean_name = clean_name[len(prefix):].strip()

            if clean_name:
                name_indexed_issues[clean_name.lower()] = issue

        elapsed = time.time() - start_time
        log_and_print(
            f"✓ Loaded {len(all_issues)} Business Services in {format_duration(elapsed)}",
            "info"
        )
        return indexed_issues, name_indexed_issues

    except Exception as e:
        log_and_print(f"✗ Error loading Business Services: {e}", "error")
        # Log more details for debug
        import traceback
        log_and_print(f"  Stack trace: {traceback.format_exc()}", "error")
        return {}, {}


def sync_business_services_from_json_optimized(json_file_path: str) -> int:
    """
    ✅ OPTIMIZED VERSION: Single-pass synchronization

    Optimized workflow:
    1. Load ALL Business Services into memory (only once)
    2. Load JSON file
    3. For each JSON record:
       - Lookup issue in cache (O(1))
       - Compare and update if needed
       - Remove/add label in the same request

    :param json_file_path: Path to JSON file
    :return: Total number of errors
    """
    program_start = time.time()

    log_and_print("\n" + "=" * 80, "info")
    log_and_print("=== BUSINESS SERVICES SYNCHRONIZATION (OPTIMIZED) ===", "info")
    log_and_print("=" * 80, "info")
    log_and_print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")
    log_and_print(f"Source file: {json_file_path}", "info")

    if DRY_RUN:
        log_and_print("⚠️  DRY RUN MODE: Simulation without real changes", "info")

    total_errors = 0

    try:
        # ✅ STEP 1: Load ALL Business Services at once
        step1_start = time.time()
        issues_cache, name_cache = load_all_business_services_with_fields()
        step1_duration = time.time() - step1_start

        if not issues_cache:
            log_and_print("⚠️  No issues loaded - cannot proceed", "warning")
            return 1

        # ✅ STEP 2: Load JSON file
        log_and_print("\n→ Loading JSON file...", "info")
        if not os.path.exists(json_file_path):
            log_and_print(f"✗ File not found: {json_file_path}", "error")
            return 1

        try:
            with open(json_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            log_and_print(f"✗ Error reading JSON: {e}", "error")
            return 1

        records = data.get("records", []) if isinstance(data, dict) else data
        log_and_print(f"✓ Loaded {len(records)} records from JSON", "info")

        # ✅ STEP 3: Identify issues to clean up
        # (those in Jira but not in JSON)
        log_and_print("\n→ Identifying issues to clean up...", "info")
        json_sys_ids = set()
        for record in records:
            sys_id = record.get("sys_id")
            if sys_id:
                json_sys_ids.add(sys_id)

        issues_to_clean = []
        for sys_id, issue in issues_cache.items():
            if sys_id not in json_sys_ids:
                # This issue is no longer in JSON - remove label
                labels = issue.get("fields", {}).get("labels", [])
                if VERIFICATION_LABEL in labels:
                    issues_to_clean.append(issue.get("key"))

        log_and_print(
            f"  • Found {len(issues_to_clean)} issues to clean (not in JSON)",
            "info"
        )

        # ✅ STEP 4: Process all JSON records
        step2_start = time.time()
        log_and_print("\n→ Processing JSON records...", "info")

        stats = {
            'created': 0,
            'updated': 0,
            'excluded': 0,
            'unchanged': 0,
            'labels_removed': 0,
            'errors': 0
        }

        for idx, record in enumerate(records, 1):
            sys_id = record.get("sys_id")
            name = record.get("name", "Unknown")

            # Check exclusions
            should_exclude, reason = should_exclude_item(record)
            if should_exclude:
                stats['excluded'] += 1
                log_and_print(f"  [{idx}/{len(records)}] ⊗ Excluded: {name} ({reason})", "debug")
                continue

            if not sys_id:
                stats['errors'] += 1
                log_and_print(f"  [{idx}/{len(records)}] ✗ Missing sys_id: {name}", "error")
                continue

            # ✅ Lookup in cache (O(1) instead of API request)
            existing_issue = issues_cache.get(sys_id)
            if not existing_issue:
                existing_issue = name_cache.get(name.lower())
                if existing_issue:
                    log_and_print(f"  → Found existing issue via name matching: {existing_issue['key']}", "debug")

            if existing_issue:
                # Update
                issue_key = existing_issue.get("key")
                log_and_print(f"  [{idx}/{len(records)}] → Processing: {name} ({issue_key})", "debug")

                success = _update_business_service_optimized(issue_key, record, existing_issue)
                if success:
                    stats['updated'] += 1
                else:
                    stats['errors'] += 1
            else:
                # Create
                log_and_print(f"  [{idx}/{len(records)}] → Creating: {name}", "info")
                issue_key = _create_business_service(record)
                if issue_key:
                    stats['created'] += 1
                    log_and_print(f"  [{idx}/{len(records)}]   ✓ Created: {issue_key}", "info")
                else:
                    stats['errors'] += 1

            # Progress
            if idx % 50 == 0:
                log_and_print(f"  → Progress: {idx}/{len(records)} records processed", "info")

        # ✅ STEP 5: Clean obsolete issues
        if issues_to_clean:
            log_and_print(f"\n→ Cleaning {len(issues_to_clean)} obsolete issues...", "info")
            for issue_key in issues_to_clean:
                if _remove_label_only(issue_key):
                    stats['labels_removed'] += 1
                else:
                    stats['errors'] += 1

        step2_duration = time.time() - step2_start
        total_duration = time.time() - program_start

        # ✅ FINAL REPORT
        log_and_print("\n" + "=" * 80, "info")
        log_and_print("=== SYNCHRONIZATION COMPLETED ===", "info")
        log_and_print("=" * 80, "info")

        log_and_print(f"\n📊 PERFORMANCE:", "info")
        log_and_print(f"  • Step 1 (Load cache):     {format_duration(step1_duration)}", "info")
        log_and_print(f"  • Step 2 (Process JSON):   {format_duration(step2_duration)}", "info")
        log_and_print(f"  " + "-" * 50, "info")
        log_and_print(f"  ⏱️  TOTAL:                  {format_duration(total_duration)}", "info")

        log_and_print(f"\n📈 RESULTS:", "info")
        log_and_print(f"  • Created: {stats['created']}", "info")
        log_and_print(f"  • Updated: {stats['updated']}", "info")
        log_and_print(f"  • Unchanged: {stats['unchanged']}", "info")
        log_and_print(f"  • Excluded: {stats['excluded']}", "info")
        log_and_print(f"  • Labels removed (obsolete): {stats['labels_removed']}", "info")
        log_and_print(f"  • Errors: {stats['errors']}", "info")

        # ✅ COMPARISON with old method
        old_api_calls = len(issues_cache) + len(records)  # Old method
        new_api_calls = 2 + stats['created'] + stats['updated'] + stats['labels_removed']  # New method
        improvement = ((old_api_calls - new_api_calls) / old_api_calls * 100) if old_api_calls > 0 else 0

        log_and_print(f"\n📊 API EFFICIENCY:", "info")
        log_and_print(f"  • Old method would use: ~{old_api_calls} API calls", "info")
        log_and_print(f"  • New method uses: ~{new_api_calls} API calls", "info")
        log_and_print(f"  • Improvement: {improvement:.1f}% reduction", "info")

        log_and_print(f"\n⏰ End: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")
        log_and_print("=" * 80 + "\n", "info")

        return stats['errors']

    except Exception as e:
        total_duration = time.time() - program_start
        log_and_print(f"\n❌ PROCESS INTERRUPTED after {format_duration(total_duration)}", "error")
        log_error_with_context(
            "sync_business_services_optimized",
            f"Critical error: {e}",
            {"duration": total_duration}
        )
        return total_errors + 1


def _update_business_service_optimized(issue_key: str, record: Dict, existing_issue: Dict) -> bool:
    """
    ✅ OPTIMIZED: Updates the issue AND handles the label in the same request
    Enforce: Unassigned and specific Reporter.
    """
    try:
        now = datetime.now().strftime("%Y-%m-%d")

        # Compare fields
        fields_to_update = _compare_fields(existing_issue, record)

        # ✅ FORCE: Always ensure Assignee is empty and Reporter is correct
        if fields_to_update is None: fields_to_update = {}

        fields_to_update["assignee"] = None
        rep_id = get_mandatory_reporter_id()
        if rep_id:
            fields_to_update["reporter"] = {"accountId": rep_id}

        # Prepare payload with label AND update
        payload = {
            "update": {
                "labels": [{"add": VERIFICATION_LABEL}],
                "comment": [{
                    "add": {
                        "body": {
                            "type": "doc",
                            "version": 1,
                            "content": [{
                                "type": "paragraph",
                                "content": [{
                                    "type": "text",
                                    "text": f"Business service info updated on {now}"
                                }]
                            }]
                        }
                    }
                }] if fields_to_update else []  # Comment only if changes
            }
        }

        if fields_to_update:
            payload["fields"] = fields_to_update

        if DRY_RUN:
            log_and_print(f"  🔍 [DRY RUN] Would update: {issue_key}", "debug")
            return True

        result = update_issue(issue_key, payload)
        return result is not None

    except Exception as e:
        log_error_with_context(
            "update_business_service_optimized",
            str(e),
            {"issue_key": issue_key}
        )
        return False


def _remove_label_only(issue_key: str) -> bool:
    """Removes only the label from an obsolete issue."""
    try:
        if DRY_RUN:
            return True

        payload = {
            "update": {
                "labels": [{"remove": VERIFICATION_LABEL}]
            }
        }

        result = update_issue(issue_key, payload)
        time.sleep(LABEL_REMOVAL_DELAY)
        return result is not None

    except Exception as e:
        log_and_print(f"  ⚠️  Error removing label from {issue_key}: {e}", "warning")
        return False


def _create_business_service(record: Dict) -> Optional[str]:
    """
    Creates a new Business Service work item.

    Reuses the existing function create_business_service_issue.
    """
    return create_business_service_issue(record)


# ✅ NEW default entry point
def sync_business_services_from_json(json_file_path: str) -> int:
    """
    Main entry point - uses the optimized version.
    """
    return sync_business_services_from_json_optimized(json_file_path)


def _remove_label_from_issue(issue_key: str) -> Tuple[bool, Optional[str]]:
    """
    Removes the "business-service-verified" label from an issue.

    :param issue_key: Issue key
    :return: Tuple (success, error_message)
    """
    try:
        if DRY_RUN:
            return True, None

        payload = {
            "update": {
                "labels": [{"remove": VERIFICATION_LABEL}]
            }
        }

        result = update_issue(issue_key, payload)

        # ✅ Use delay from config
        time.sleep(LABEL_REMOVAL_DELAY)

        return result is not None, None

    except Exception as e:
        return False, str(e)


# ✅ REMOVED: Duplicate search_issue_by_sys_id_api() function


def search_issue_by_business_service_id(sys_id: str) -> Optional[Dict]:
    """
    Search for a work item by its Business service id.

    ✅ Uses centralized function from jira_utils
    """
    return search_issue_by_sys_id(
        sys_id,
        PROJECT_KEY,
        ISSUE_TYPE,
        CUSTOM_FIELDS.get("business_service_jql")
    )


def create_business_service_issue(record: Dict) -> Optional[str]:
    """
    STEP 2a.i: Creates a new Business Service work item.

    This function:
    1. Creates the work item with all fields
    2. Sets the "business-service-verified" label
    3. Adds a comment "Business service info created on" + Date
    4. ✅ FORCE: Assignee = Unassigned, Reporter = atlassians.actions@minfin.fed.be

    :param record: ServiceNow JSON record
    :return: Created issue key or None
    """
    try:
        now = datetime.now().strftime("%Y-%m-%d")

        # Construct summary: "name | sys_id"
        name = record.get("name", "Unknown")
        sys_id = record.get("sys_id", "Unknown")
        summary = f"{name} | {sys_id}"[:255]

        # Build fields
        fields = {
            "project": {"key": PROJECT_KEY},
            "issuetype": {"name": ISSUE_TYPE},
            "summary": summary,
            "labels": [VERIFICATION_LABEL],
            "assignee": None  # ✅ FORCE: Always unassigned
        }

        # ✅ FORCE: Mandatory reporter
        rep_id = get_mandatory_reporter_id()
        if rep_id:
            fields["reporter"] = {"accountId": rep_id}
            log_and_print(f"  → Setting mandatory reporter for AMI issue", "debug")

        # ✅ Description (short_description) - ATLASSIAN DOCUMENT FORMAT
        if record.get("short_description"):
            # Convert plain text to Atlassian Document format
            fields["description"] = {
                "type": "doc",
                "version": 1,
                "content": [{
                    "type": "paragraph",
                    "content": [{
                        "type": "text",
                        "text": record.get("short_description")
                    }]
                }]
            }

        # Business domain id (12657)
        if record.get("u_business_domain"):
            fields[CUSTOM_FIELDS["business_domain_id"]] = record.get("u_business_domain")

        # Business service id (10519) - sys_id
        if sys_id != "Unknown":
            fields[CUSTOM_FIELDS["business_service_id"]] = sys_id

        # Business service name (10489)
        if name != "Unknown":
            fields[CUSTOM_FIELDS["business_service_name"]] = name

        # Business service short name (12823)
        if record.get("u_short_service_id"):
            fields[CUSTOM_FIELDS["business_service_short_name"]] = record.get("u_short_service_id")

        # Business service status (10541)
        if record.get("operational_status"):
            status_value = map_operational_status(record.get("operational_status"))
            if status_value:
                fields[CUSTOM_FIELDS["business_service_status"]] = {"value": status_value}

        # Business service usage (10542)
        if record.get("used_for"):
            fields[CUSTOM_FIELDS["business_service_usage"]] = {"value": record.get("used_for")}

        # Business service classification (11623)
        if record.get("service_classification"):
            fields[CUSTOM_FIELDS["business_service_classification"]] = {
                "value": record.get("service_classification")
            }

        payload = {"fields": fields}

        # DRY RUN mode
        if DRY_RUN:
            log_and_print(f"  🔍 [DRY RUN] Would create: {summary}", "info")
            return "DRY-RUN-KEY"

        # Create issue
        result = make_jira_request(
            "POST",
            "/rest/api/3/issue",
            data=payload,
            log_function=lambda msg, level="info": log_and_print(msg, level)
        )

        if not result:
            return None

        issue_key = result.get("key")

        # Add comment
        comment_text = f"Business service info created on {now}"
        _add_comment_to_issue(issue_key, comment_text)

        return issue_key

    except Exception as e:
        log_error_with_context(
            "create_business_service",
            str(e),
            {"record": record}
        )
        return None

def _compare_fields(existing_issue: Dict, record: Dict) -> Dict:
    """
    Compare fields between Jira and ServiceNow.

    :param existing_issue: Existing Jira issue
    :param record: ServiceNow JSON record
    :return: Dictionary of fields to update
    """
    fields = existing_issue.get("fields", {})
    updates = {}

    # Field mappings to compare
    field_mappings = {
        CUSTOM_FIELDS["business_domain_id"]: {
            "json_key": "u_business_domain",
            "type": "text"
        },
        CUSTOM_FIELDS["business_service_name"]: {
            "json_key": "name",
            "type": "text"
        },
        CUSTOM_FIELDS["business_service_short_name"]: {
            "json_key": "u_short_service_id",
            "type": "text"
        },
        CUSTOM_FIELDS["business_service_status"]: {
            "json_key": "operational_status",
            "type": "select",
            "mapper": True  # ✅ Marker to indicate mapping needed
        },
        CUSTOM_FIELDS["business_service_usage"]: {
            "json_key": "used_for",
            "type": "select"
        },
        CUSTOM_FIELDS["business_service_classification"]: {
            "json_key": "service_classification",
            "type": "select"
        }
    }

    for field_id, config in field_mappings.items():
        json_key = config["json_key"]
        field_type = config["type"]

        # Value in ServiceNow
        new_value = record.get(json_key)

        # ✅ CORRECTION: Apply mapper only for business_service_status
        if config.get("mapper") and json_key == "operational_status" and new_value is not None:
            # Call imported function map_operational_status directly
            new_value = map_operational_status(new_value)

        # Current value in Jira
        current_value = fields.get(field_id)
        if isinstance(current_value, dict):
            current_value = current_value.get("value") or current_value.get("name")

        # Normalize values
        current_value = _normalize_value(current_value)
        new_value = _normalize_value(new_value)

        # Compare
        if current_value != new_value:
            if field_type == "select" and new_value:
                updates[field_id] = {"value": new_value}
            elif field_type == "text" and new_value:
                updates[field_id] = new_value

    # ✅ FIX: Handle "Retired-" prefix in summary idempotently
    operational_status = record.get("operational_status")
    current_summary = fields.get("summary", "")

    # ✅ Extract "base" part of summary (without Retired prefix)
    # Remove all possible patterns: "Retired-", "Retired - ", etc.
    base_summary = current_summary
    while base_summary.startswith("Retired-") or base_summary.startswith("Retired - "):
        if base_summary.startswith("Retired - "):
            base_summary = base_summary[len("Retired - "):].strip()
        elif base_summary.startswith("Retired-"):
            base_summary = base_summary[len("Retired-"):].strip()

    if operational_status == "6":
        # Service becomes/remains Retired
        # ✅ Build new summary with ONLY ONE clean prefix
        expected_summary = f"Retired-{base_summary}"[:255]

        # ✅ Update only if different
        if current_summary != expected_summary:
            updates["summary"] = expected_summary
            log_and_print(
                f"  → Summary will be updated to mark as Retired: '{expected_summary}'",
                "info"
            )
    else:
        # Service is no longer Retired - remove all prefixes
        if current_summary != base_summary:
            updates["summary"] = base_summary[:255]
            log_and_print(
                f"  → Summary will be updated to remove Retired prefix: '{base_summary}'",
                "info"
            )

    return updates

def _normalize_value(value) -> Optional[str]:
    """Normalizes a value for comparison."""
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str if value_str else None


def _add_label_to_issue(issue_key: str, label: str) -> bool:
    """Adds a label to an issue."""
    try:
        if DRY_RUN:
            return True

        payload = {
            "update": {
                "labels": [{"add": label}]
            }
        }

        result = update_issue(issue_key, payload)
        return result is not None

    except Exception as e:
        log_and_print(f"  ⚠️  Error adding label {issue_key}: {e}", "warning")
        return False


def _add_comment_to_issue(issue_key: str, comment_text: str) -> bool:
    """Adds a comment to an issue."""
    try:
        if DRY_RUN:
            return True

        payload = {
            "body": {
                "type": "doc",
                "version": 1,
                "content": [{
                    "type": "paragraph",
                    "content": [{
                        "type": "text",
                        "text": comment_text
                    }]
                }]
            }
        }

        result = make_jira_request(
            "POST",
            f"/rest/api/3/issue/{issue_key}/comment",
            data=payload,
            log_function=lambda msg, level="info": log_and_print(msg, level)
        )

        return result is not None

    except Exception as e:
        log_and_print(f"  ⚠️  Error adding comment {issue_key}: {e}", "warning")
        return False

