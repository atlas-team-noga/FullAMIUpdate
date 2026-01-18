"""
Module for interacting with Jira API to perform issue searches, updates, and other operations.

This module provides utility functions to work with Jira, including searching for issues using
JQL with pagination, sending generic HTTP requests, and updating Jira issues with retry logic
to handle rate limits and recoverable errors.
"""

import requests
import json
import time
from config_utils import JIRA_URL, AUTH, HEADERS
from logger_utils import log_and_print, log_error_with_context
from rate_limit_config import RATE_LIMIT_DELAY, MAX_RETRIES, RETRY_BASE_DELAY


def fetch_jira_issues_paginated(jql):
    """
    Fetches paginated Jira issues based on the provided JQL query.

    This function retrieves Jira issues using the Jira REST API in a paginated
    manner. The search query is specified using JQL (Jira Query Language). It
    handles pagination by iterating through the pages until all issues are
    retrieved or no more pages are available. The function returns a list of
    issues retrieved during the process.

    It logs the progress and total number of issues found, ensuring that no
    issues are missed. Any unexpected errors or HTTP-related exceptions are caught
    and appropriately logged, ensuring robustness.

    :param jql: The JQL query string used to filter and retrieve Jira issues
                from the Jira REST API.
    :type jql: str

    :return: A list of issues retrieved for the specified JQL query.
    :rtype: list
    """
    try:
        url = f"{JIRA_URL}/rest/api/3/search/jql"

        all_issues = []
        next_page_token = None
        max_results = 100
        total = None

        log_and_print(f"Searching Business Services with JQL: {jql}", "info")

        while True:
            payload = {
                "jql": jql,
                "maxResults": max_results,
                "fields": ["key", "labels"],
                "fieldsByKeys": True
            }

            # Ajouter le nextPageToken si présent (sauf première itération)
            if next_page_token:
                payload["nextPageToken"] = next_page_token

            log_and_print(f"Fetching page (token: {next_page_token if next_page_token else 'first page'})...", "debug")

            response = requests.request(
                "POST",
                url,
                data=json.dumps(payload),
                headers=HEADERS,
                auth=AUTH
            )
            response.raise_for_status()

            result = response.json()
            issues = result.get("issues", [])
            all_issues.extend(issues)

            # Get total on first request
            if total is None:
                total = result.get("total", 0)
                log_and_print(f"Total Business Services found: {total}", "info")

            current_count = len(all_issues)
            log_and_print(f"Progress: {current_count}/{total} issues retrieved", "info")

            # Récupérer le nextPageToken pour la page suivante
            next_page_token = result.get("nextPageToken")

            # Si nextPageToken est null, c'est la dernière page
            if not next_page_token:
                log_and_print("Last page reached (nextPageToken is null)", "debug")
                break

            # Sécurité: si aucune issue retournée, arrêter
            if len(issues) == 0:
                log_and_print("No more issues returned", "debug")
                break

        log_and_print(f"✓ Retrieval completed: {len(all_issues)} Business Services total", "info")
        return all_issues

    except requests.exceptions.HTTPError as er:
        log_and_print(f"HTTP error while searching Business Services: {er}", "error")
        if er.response is not None:
            log_and_print(f"Contenu de la réponse: {er.response.text}", "error")
        return []
    except Exception as er:
        log_and_print(f"Erreur lors de la recherche des Business Services: {er}", "error")
        return []

def make_jira_request(method, endpoint, data=None, params=None, log_function=None):
    """
    Makes an HTTP request to a JIRA server endpoint using the specified HTTP method.

    This function provides a wrapper for making HTTP requests to a JIRA instance, supporting
    common methods such as GET, POST, PUT, and DELETE. It handles JSON encoding of request
    bodies when applicable and processes query parameters. HTTP errors and other exceptions
    are caught and logged using the provided logging function or a default implementation.
    The function ultimately returns the JSON response from the server or an empty dictionary
    if there is no content.

    :param method: The HTTP method to use for the request (e.g., 'GET', 'POST', 'PUT', 'DELETE').
    :type method: str
    :param endpoint: The JIRA API endpoint to be accessed (relative to the base URL).
    :type endpoint: str
    :param data: The data to be sent in the request body, if applicable (optional).
    :type data: dict or None
    :param params: The query parameters to include in the request URL (optional).
    :type params: dict or None
    :param log_function: A callable for logging messages, taking the message and optional
        log level as arguments (optional). If not provided, a default logger is used.
    :type log_function: callable or None

    :return: The parsed JSON response from the server if successful, or an empty dictionary
        if the response body is empty. Returns `None` if an error occurs.
    :rtype: dict or None
    """
    if log_function is None:
        log_function = lambda msg, level="info": print(msg)

    try:
        url = f"{JIRA_URL}{endpoint}"

        if method.upper() == "GET":
            response = requests.get(url, headers=HEADERS, auth=AUTH, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=HEADERS, auth=AUTH, data=json.dumps(data) if data else None)
        elif method.upper() == "PUT":
            response = requests.put(url, headers=HEADERS, auth=AUTH, data=json.dumps(data) if data else None)
        elif method.upper() == "DELETE":
            response = requests.delete(url, headers=HEADERS, auth=AUTH)
        else:
            log_function(f"✗ Unsupported HTTP method: {method}", "error")
            return None

        response.raise_for_status()
        return response.json() if response.text else {}

    except requests.exceptions.HTTPError as e:
        log_function(f"✗ HTTP Error ({method} {endpoint}): {e}", "error")
        if hasattr(e, 'response') and e.response is not None:
            log_function(f"  Response: {e.response.text}", "error")
        return None
    except Exception as e:
        log_function(f"✗ Error ({method} {endpoint}): {e}", "error")
        return None

def search_issue_by_sys_id(sys_id, project_key, issue_type, custom_field_id):
    """
    Searches for a JIRA issue based on a system ID, project key, issue type, and custom field.

    This function uses a JIRA Query Language (JQL) query to search for a specific issue in
    a project that matches the given parameters. The search excludes specific issue keys
    defined in the query. It returns the first matching issue, if any are found. Otherwise,
    it returns None.

    :param sys_id: The system ID to search for in the custom field.
    :type sys_id: str
    :param project_key: The project key representing the JIRA project to search within.
    :type project_key: str
    :param issue_type: The type of issue to look for, such as "Bug" or "Task".
    :type issue_type: str
    :param custom_field_id: The custom field ID to match the system ID against.
    :type custom_field_id: str
    :return: The first issue found matching the search parameters, or None if no issues are found.
    :rtype: dict or None
    """
    try:
        jql = (f'project = {project_key} AND '
               f'issuetype = "{issue_type}" AND '
               f'"{custom_field_id}" ~ "{sys_id}" AND '
               f'key NOT IN (AMI-4925, AMI-4926, AMI-4927)')

        url = f"{JIRA_URL}/rest/api/3/search/jql"

        payload = json.dumps({
            "jql": jql,
            "fields": ["*all"],
            "fieldsByKeys": True,
            "maxResults": 1
        })

        response = requests.post(
            url,
            data=payload,
            headers=HEADERS,
            auth=AUTH
        )
        response.raise_for_status()

        result = response.json()
        issues = result.get("issues", [])

        return issues[0] if issues else None

    except Exception as e:
        log_and_print(f"  ⚠️ Error searching for sys_id {sys_id}: {e}", "warning")
        return None


def update_issue(issue_key, data):
    """
    Updates a JIRA issue with the provided data. The function contains retry logic to handle HTTP 429 (Too Many
    Requests) responses with exponential backoff. If it exceeds the maximum retries or encounters non-recoverable
    errors, it logs detailed error information for debugging and diagnostics. A delay is introduced after successful
    requests to minimize the possibility of exceeding rate limits.

    :param issue_key: The key of the JIRA issue to update (e.g., 'PROJ-123').
    :type issue_key: str
    :param data: The payload containing the fields to update in the JIRA issue.
    :type data: dict
    :return: The HTTP status code of the successful update operation, or None if the update fails permanently.
    :rtype: int or None
    """
    # ✅ Retry logic for 429 errors
    for attempt in range(MAX_RETRIES):
        try:
            url = f"{JIRA_URL}/rest/api/3/issue/{issue_key}"

            log_and_print(f"  → Updating {issue_key} with payload: {json.dumps(data, indent=2)}", "debug")

            response = requests.put(url, headers=HEADERS, auth=AUTH, data=json.dumps(data))
            
            # ✅ Handle 429 specifically with exponential backoff
            if response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    retry_delay = RETRY_BASE_DELAY * (2 ** attempt)
                    log_and_print(
                        f"  ⚠️  HTTP 429 (Too Many Requests) for {issue_key}. "
                        f"Retrying in {retry_delay}s (attempt {attempt + 1}/{MAX_RETRIES})...",
                        "warning"
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    log_and_print(
                        f"  ✗ HTTP 429 - Max retries ({MAX_RETRIES}) exceeded for {issue_key}",
                        "error"
                    )
            
            response.raise_for_status()
            
            # ✅ Add delay after successful request to prevent rate limiting
            time.sleep(RATE_LIMIT_DELAY)
            
            return response.status_code

        except requests.exceptions.HTTPError as er:
            # Si c'est le dernier essai ou pas une erreur 429, on traite l'erreur normalement
            if er.response and er.response.status_code != 429:
                http_status = er.response.status_code
                try:
                    response_text = er.response.text
                    response_json = er.response.json()
                except:
                    response_text = er.response.text if hasattr(er.response, 'text') else "Unable to read response"
                    response_json = None

                log_and_print(f"  ✗ HTTP {http_status} updating {issue_key}", "error")
                if response_json:
                    log_and_print(f"  → Error details: {json.dumps(response_json, indent=2)}", "error")
                elif response_text:
                    log_and_print(f"  → Response: {response_text[:500]}", "error")
                log_and_print(f"  → Payload sent: {json.dumps(data, indent=2)}", "error")

                error_details = {
                    "issue_key": issue_key,
                    "http_status": http_status,
                    "response": response_text[:1000] if response_text else None,
                    "response_json": response_json,
                    "payload": data
                }
                log_error_with_context("update_issue", f"HTTP Error {er}", error_details)
                return None
            
            # Si c'est une 429 et qu'on a épuisé les tentatives, on abandonne
            if attempt == MAX_RETRIES - 1:
                log_and_print(f"  ✗ Failed to update {issue_key} after {MAX_RETRIES} attempts", "error")
                return None

        except Exception as er:
            error_details = {
                "issue_key": issue_key,
                "exception_type": type(er).__name__,
                "exception_message": str(er),
                "payload": data
            }
            log_error_with_context("update_issue", str(er), error_details)
            return None
    
    return None
