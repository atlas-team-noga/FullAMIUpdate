"""
A utility for managing project configurations and determining exclusion conditions.

This module loads project and configuration data and provides a function to evaluate
items for exclusion based on their properties.
"""

from config_utils import get_project_config, load_config_json

# Charger la config une seule fois
_project_config = get_project_config()
PROJECT_KEY = _project_config["project_key"]
ISSUE_TYPE = _project_config["issue_type"]
CUSTOM_FIELDS = _project_config["custom_fields"]
REPORTER_ACCOUNT_ID = _project_config.get("reporter_account_id")

# Charger aussi la config complète pour accès aux autres sections
CONFIG = load_config_json()


def should_exclude_item(item):
    """
    Determines if a given item should be excluded based on specific conditions.

    This function checks the item's ``name`` property and returns a tuple indicating
    whether the item should be excluded and a reason for the exclusion. The function
    only excludes items if the name explicitly meets the predefined condition.

    :param item: A dictionary representing the item to check. It is expected to
        contain a ``name`` key, which is used to decide exclusion.
    :type item: dict
    :return: A tuple where the first value is a boolean indicating if the item should
        be excluded, and the second value is a string providing the reason for exclusion.
    :rtype: tuple[bool, str]
    """
    # ✅ Pour le dropdown cascadé, on ne filtre PAS par issue_key
    # Les tickets AMI ne sont exclus que dans Business_service.py

    name = item.get("name", "")

    if name == "All":
        return True, "name='All'"

    return False, ""