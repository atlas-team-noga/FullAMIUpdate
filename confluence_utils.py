"""
Provides functionality to interact with Confluence pages through API calls.

This module allows the retrieval, searching, and updating of Confluence page
content using REST API endpoints. It utilizes the requests library for HTTP
calls and BeautifulSoup for parsing HTML content within Confluence pages.
"""

import requests
from bs4 import BeautifulSoup
from config_utils import JIRA_URL, AUTH, HEADERS
from logger_utils import log_and_print, log_error_with_context


def get_confluence_page_content(page_id):
    """
    Fetches the content of a Confluence page, parses its HTML, and retrieves additional details such as
    the page's current version.

    This function connects to the Confluence instance using the supplied credentials and URL, fetches
    the specified page's content through its REST API, and processes it to return useful data
    structures for further operations.

    :param page_id: The unique identifier of the Confluence page.
    :type page_id: str
    :return: A tuple containing the following:
        - page_data (dict | None): The raw JSON response from the Confluence API representing the page data.
          Returns None if an error occurs.
        - current_version (int | None): The current version of the Confluence page. Returns None if an
          error occurs.
        - html_content (str | None): The raw HTML content of the Confluence page as a string. Returns
          None if an error occurs.
        - soup (BeautifulSoup | None): A BeautifulSoup object representing the parsed HTML content of
          the page. Returns None if an error occurs.
    :rtype: tuple[dict | None, int | None, str | None, BeautifulSoup | None]
    """
    try:
        url = f"{JIRA_URL}/wiki/rest/api/content/{page_id}?expand=body.storage,version"
        response = requests.get(url, headers=HEADERS, auth=AUTH)
        response.raise_for_status()

        page_data = response.json()
        current_version = page_data['version']['number']
        html_content = page_data['body']['storage']['value']
        soup = BeautifulSoup(html_content, 'html.parser')

        return page_data, current_version, html_content, soup
    except Exception as err:
        log_error_with_context("get_confluence_page", str(err), {
            "page_id": page_id,
            "exception_type": type(err).__name__
        })
        return None, None, None, None


def find_confluence_table(soup, table_id):
    """
    Finds a Confluence table within the provided HTML or XML structure using the
    specified table ID.

    This function looks for a table with the 'ac:local-id' attribute matching the
    provided ID within the given HTML/XML object and returns the table element if
    found. If the table is not located, an error message is logged and printed.

    :param soup: BeautifulSoup object representing the parsed HTML or XML content
        of a Confluence page.
    :type soup: BeautifulSoup
    :param table_id: The unique identifier for the desired Confluence table.
    :type table_id: str
    :return: The corresponding table element if found, otherwise None.
    :rtype: Tag or None
    """
    table = soup.find('table', {'ac:local-id': table_id})
    if not table:
        log_and_print(f"  ✗ Table with ID {table_id} not found in Confluence page", "error")
    return table


def update_confluence_page(page_id, page_data, current_version, updated_html, commit_message):
    """
    Updates an existing Confluence page with the provided HTML content, title, and metadata.

    This function handles the process of updating a Confluence page by making an
    authenticated HTTP request to the Confluence REST API. It increments the page
    version, updates the content with the new HTML value, and assigns any provided
    commit message for versioning purposes.

    :param page_id: The unique identifier of the Confluence page to be updated.
    :param page_data: A dictionary containing details about the page,
        including its title.
    :param current_version: The current version of the page incremented before
        the update.
    :param updated_html: The new HTML content that will replace the existing
        content on the page.
    :param commit_message: A message describing the changes in the update, stored
        with the new page version.
    :return: Returns ``True`` if the page was successfully updated; otherwise,
        returns ``False`` if an HTTP or general exception occurred.
    """
    try:
        update_data = {
            "id": page_id,
            "type": "page",
            "title": page_data['title'],
            "version": {
                "number": current_version + 1,
                "message": commit_message
            },
            "body": {
                "storage": {
                    "value": updated_html,
                    "representation": "storage"
                }
            }
        }

        update_url = f"{JIRA_URL}/wiki/rest/api/content/{page_id}"
        import json
        update_response = requests.put(
            update_url,
            headers=HEADERS,
            auth=AUTH,
            data=json.dumps(update_data)
        )
        update_response.raise_for_status()

        log_and_print(f"  ✓ Page updated to version {current_version + 1}", "info")
        return True
    except requests.exceptions.HTTPError as http_err:
        log_and_print(f"  ✗ HTTP Error updating Confluence: {http_err}", "error")
        if http_err.response:
            log_and_print(f"  → Response: {http_err.response.text[:500]}", "error")
        return False
    except Exception as err:
        log_error_with_context("update_confluence_page", str(err), {
            "exception_type": type(err).__name__
        })
        return False
