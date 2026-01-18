"""
A script for automating business service operations, including configuration parsing,
label removal, and optional reporter account ID resolution.

The script provides modular utilities for operational status mapping, processing email-based
account IDs, and centralized logging to support downstream workflows. It also supports
dry run mode for simulation without real modifications.
"""

import requests
import logging
from datetime import datetime
import time
from config_utils import get_project_config, DRY_RUN, JIRA_URL, AUTH, HEADERS
from rate_limit_config import MAX_WORKERS
from common_functions import REPORTER_ACCOUNT_ID, CONFIG
from logger_utils import log_and_print, log_error_with_context, format_duration

project_cfg = get_project_config()
CASCADING_DROPDOWN_FIELD_ID = CONFIG["dropdown_cascading_field"]["bus_domain_and_service"]

# Global variable to store the resolved accountId
RESOLVED_REPORTER_ACCOUNT_ID = None

# Reusable HTTP session
jira_url = JIRA_URL
auth = AUTH
SESSION = requests.Session()
SESSION.auth = auth
SESSION.headers.update(HEADERS)


# === LOGGING SETUP ===
main_logger = logging.getLogger('main')
main_logger.setLevel(logging.INFO)

main_handler = logging.FileHandler('business_service_automation.log', encoding='utf-8')
main_handler.setLevel(logging.INFO)
main_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
main_logger.addHandler(main_handler)

error_logger = logging.getLogger('errors')
error_logger.setLevel(logging.ERROR)

error_handler = logging.FileHandler('business_service_errors.log', encoding='utf-8')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
error_logger.addHandler(error_handler)


def map_operational_status(status_code):
    """
    Maps a given operational status code to its corresponding descriptive string. The function takes
    an operational status code as input and returns a string that describes the operational
    status with the code included in parentheses. If the status code is `None`, the function
    returns `None`. If the status code is not found in the predefined mapping, the function
    returns `None`.

    :param status_code: The operational status code to be mapped. Can be of any type that can
        be converted to a string.
    :return: A string representing the mapped operational status, including the status code in
        parentheses, or `None` if the status code is not mapped or if `None` is given as input.
    """
    status_mapping = {
        "1": "Operational (1)",
        "2": "Non-Operational (2)",
        "3": "Repair in Progress (3)",
        "4": "DR Standby (4)",
        "5": "Ready (5)",
        "6": "Retired (6)",
        "20": "Planned (20)"
    }

    if status_code is None:
        return None

    return status_mapping.get(str(status_code))

def remove_verification_labels():
    """
    Removes verification labels from the current context.

    This function acts as a no-op because labels are managed by a centralized
    workflow. It logs the action using the logging infrastructure if it is
    initialized; otherwise, it defaults to using a plain print statement.

    :return: None
    """
    try:
        log_and_print("ℹ remove_verification_labels(): no-op — labels are managed by the centralized workflow.", "info")
    except Exception:
        # Fallback to plain print if logging isn't initialized yet
        print("remove_verification_labels(): no-op — labels are managed by the centralized workflow.")

def get_account_id_by_email(email):
    """
    Get the account identifier associated with a given email from the JIRA platform.

    The function queries the JIRA REST API to find the user's account ID based on the provided email address.
    If an exact match for the email is found, the corresponding account ID is returned. If no exact match is
    found but users are present in the result, it returns the account ID of the first user as an approximate match.
    In case of errors during the API call or if no users are found for the query, it returns None.

    :param email: The email address of the user for which the JIRA account ID should be retrieved.
    :type email: str
    :return: The account ID associated with the given email, or None if no match is found or in case of an error.
    :rtype: str or None
    """
    try:
        url = f"{jira_url}/rest/api/3/user/search"
        params = {"query": email}

        response = SESSION.get(url, params=params)
        response.raise_for_status()

        users = response.json()
        if users and len(users) > 0:
            # Search for user with exact email
            for user in users:
                if user.get("emailAddress", "").lower() == email.lower():
                    account_id = user.get("accountId")
                    log_and_print(f"  ✓ AccountId found for {email}: {account_id}", "debug")
                    return account_id

            # If no exact match, take the first result
            account_id = users[0].get("accountId")
            log_and_print(f"  ⚠ Approximate AccountId for {email}: {account_id}", "warning")
            return account_id

        log_and_print(f"  ✗ No user found for email: {email}", "warning")
        return None

    except Exception as er:
        log_and_print(f"  ✗ Error searching for user {email}: {er}", "warning")
        return None


def resolve_reporter_account_id():
    """
    Resolves the reporter account ID based on the provided configuration.

    This function determines the appropriate account ID to use when creating
    issues. If the configured reporter_account_id is in email format, it searches
    for the related account ID. If an account ID cannot be found for the provided
    email, issues will be created without a specific reporter. If the
    reporter_account_id is already an account ID, it is validated and returned
    without further processing.

    :returns: The resolved account ID if successfully identified, or ``None`` if
              no valid account ID can be determined.
    :rtype: str or None
    """
    global RESOLVED_REPORTER_ACCOUNT_ID

    if not REPORTER_ACCOUNT_ID:
        log_and_print("⚠ No reporter_account_id configured in config.json", "warning")
        RESOLVED_REPORTER_ACCOUNT_ID = None
        return None

    # Check if it's an email (contains @)
    if "@" in REPORTER_ACCOUNT_ID:
        log_and_print(f"🔍 Email detected in reporter_account_id: {REPORTER_ACCOUNT_ID}", "info")
        log_and_print(f"   Searching for corresponding accountId...", "info")

        account_id = get_account_id_by_email(REPORTER_ACCOUNT_ID)

        if account_id:
            log_and_print(f"✓ AccountId found: {account_id}", "info")
            log_and_print(f"💡 Tip: Update your config.json with this accountId to avoid this search on every execution.", "info")
            RESOLVED_REPORTER_ACCOUNT_ID = account_id
            return account_id
        else:
            log_and_print(f"✗ Unable to find accountId for {REPORTER_ACCOUNT_ID}", "error")
            log_and_print(f"  Issues will be created without a specific reporter.", "warning")
            RESOLVED_REPORTER_ACCOUNT_ID = None
            return None
    else:
        # It's already an accountId
        log_and_print(f"✓ Reporter accountId configured: {REPORTER_ACCOUNT_ID}", "info")
        RESOLVED_REPORTER_ACCOUNT_ID = REPORTER_ACCOUNT_ID
        return REPORTER_ACCOUNT_ID

# === MAIN EXECUTION ===
if __name__ == "__main__":
    # Start the global timer
    program_start_time = time.time()

    try:
        log_and_print("=== Starting Business Service Automation process ===", "info")
        log_and_print(f"Start Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")
        if DRY_RUN:
            log_and_print("⚠️  DRY RUN MODE ENABLED - Simulation without real modifications", "info")
        log_and_print(f"Configuration:", "info")
        log_and_print(f"  - Parallel workers: {MAX_WORKERS}", "info")
        log_and_print(f"Log files:", "info")
        log_and_print(f"  - Main log: business_service_automation.log", "info")
        log_and_print(f"  - Error log: business_service_errors.log", "info")

        # Note: Log files are reset by main_workflow.py if executed via the main workflow

        # Step 0: Resolve reporter accountId
        log_and_print("\n=== Step 0: Reporter configuration ===", "info")
        step0_start = time.time()
        resolve_reporter_account_id()
        step0_duration = time.time() - step0_start

        # Step 1: Remove verification labels
        log_and_print("\n", "info")
        step1_start = time.time()
        remove_verification_labels()
        step1_duration = time.time() - step1_start

        # Step 2: Process JSON file
        json_file = "servicenow_cmdb_ci_service_20251103_133624.json"
        step2_start = time.time()
        # ✅ Get error_count from process_json_file
        # final_error_count = process_json_file(json_file)
        step2_duration = time.time() - step2_start

        # Calculate total duration
        total_duration = time.time() - program_start_time

        log_and_print("\n" + "=" * 70, "info")
        log_and_print("=== 🎉 PROCESS COMPLETED SUCCESSFULLY ===", "info")
        log_and_print("=" * 70, "info")

        log_and_print(f"\n📊 DETAILED PERFORMANCE REPORT:", "info")
        log_and_print(f"  • Step 0 (Configuration):          {format_duration(step0_duration)}", "info")
        log_and_print(f"  • Step 1 (Label removal):          {format_duration(step1_duration)}", "info")
        log_and_print(f"  • Step 2 (File processing):        {format_duration(step2_duration)}", "info")
        log_and_print(f"  " + "-" * 50, "info")
        log_and_print(f"  ⏱️  TOTAL EXECUTION DURATION:        {format_duration(total_duration)}", "info")

        # Calculate percentages
        if total_duration > 0:
            step1_percent = (step1_duration / total_duration) * 100
            step2_percent = (step2_duration / total_duration) * 100
            log_and_print(f"\n📈 TIME DISTRIBUTION:", "info")
            log_and_print(f"  • Label removal:  {step1_percent:.1f}%", "info")
            log_and_print(f"  • File processing:  {step2_percent:.1f}%", "info")

        log_and_print(f"\n⏰ End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "info")

        # ✅ Check with actual error count from this execution
        log_and_print(f"\n📝 LOG STATUS:", "info")
        # if final_error_count and final_error_count > 0:
        #     log_and_print(
        #         f"  ⚠️  {final_error_count} error(s) detected. Check business_service_errors.log for details.",
        #         "warning")
        # else:
        #     log_and_print("  ✅ No errors detected during this execution.", "info")

        log_and_print("\n" + "=" * 70 + "\n", "info")

    except Exception as err:
        total_duration = time.time() - program_start_time
        log_and_print(f"\n❌ PROCESS INTERRUPTED after {format_duration(total_duration)}", "error")
        log_error_with_context(
            "main_execution",
            f"Critical error: {err}",
            {"exception_type": type(err).__name__, "duration": total_duration}
        )
        raise