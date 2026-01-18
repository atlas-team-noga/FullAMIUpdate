"""
A program to load and validate environment variables, handle configuration from JSON files,
and provide JIRA project-specific details for integration. The program ensures all necessary
environment variables and configuration files are present and correctly structured.

The module validates `.env` files, environment variable availability, and JSON configurations
for required settings. Additionally, it defines functions for loading JSON configurations and
retrieving JIRA project-specific settings.
"""

import os
import json
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# Load environment variables
dotenv_path = '.venv/Lib/site-ini.env'
load_dotenv(dotenv_path)

# Validate .env file existence
if not os.path.exists(dotenv_path):
    raise FileNotFoundError(
        f"Error: .env file not found at {dotenv_path}. "
        "Please ensure the .env file exists."
    )

# Environment variables
JIRA_URL = os.getenv("JIRA_URL")
USERNAME = os.getenv("EMAIL")
API_TOKEN = os.getenv("API_TOKEN")
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
SCHEDULED_TIME = os.getenv("SCHEDULED_TIME", "6:00am")
DRY_RUN = os.getenv("DRY_RUN", "False").lower() in ("true", "1", "yes")

# Authentication
AUTH = HTTPBasicAuth(USERNAME, API_TOKEN)
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Validate required environment variables
if not all([JIRA_URL, USERNAME, API_TOKEN]):
    raise ValueError(
        "Missing environment variables. "
        "Ensure JIRA_URL, EMAIL and API_TOKEN are defined in .env"
    )

print(f"✓ Configuration loaded from: {dotenv_path}")
print(f"✓ Connecting to: {JIRA_URL}")
if DRY_RUN:
    print(f"⚠️  DRY RUN MODE ENABLED - No modifications will be applied")


def load_config_json(config_file="config.json"):
    """
    Load configuration from a JSON file.

    This function attempts to load a JSON configuration from the given file path.
    If the file does not exist, it raises a `FileNotFoundError`. If there is any
    error in reading or parsing the JSON file, it raises a `ValueError`.

    :param config_file: Path to the JSON configuration file. Defaults to "config.json".
                        This file must follow the structure defined in the README.md.
    :type config_file: str
    :return: Parsed JSON configuration data.
    :rtype: dict
    :raises FileNotFoundError: If the specified config file does not exist.
    :raises ValueError: If there is an error reading or parsing the JSON configuration.
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(
            f"Error: {config_file} not found. "
            "Please create this file according to README.md instructions."
        )

    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✓ JSON configuration loaded from: {config_file}")
        return config
    except Exception as e:
        raise ValueError(f"Error loading {config_file}: {e}")


def get_project_config():
    """
    Retrieve the project configuration details from a JSON configuration file.

    This function loads the configuration file, extracts JIRA project-specific details, and
    returns the configuration in a structured format. The returned data includes project key,
    issue type, custom fields, Confluence settings, an optional reporter account ID, and an
    optional dropdown cascading field.

    :return: A dictionary containing the extracted project configuration details. The keys
             in the dictionary are 'project_key', 'issue_type', 'custom_fields', 'confluence',
             'reporter_account_id', and 'dropdown_cascading_field'.
    :rtype: dict
    """
    config = load_config_json()
    return {
        "project_key": config["jira"]["project_key"],
        "issue_type": config["jira"]["issue_type"],
        "custom_fields": config["custom_fields"],
        "confluence": config.get("confluence", {}),
        "reporter_account_id": config["jira"].get("reporter_account_id"),
        "dropdown_cascading_field": config.get("dropdown_cascading_field", {})
    }
