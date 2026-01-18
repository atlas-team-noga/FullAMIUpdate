"""
Configuration for API request rate limiting and parallelism adjustments.

This module contains configuration constants used to manage rate-limiting
and optimize parallel execution for API requests. Rate limiting is applied
to control the frequency of outgoing requests, and retries are used to
handle temporary errors. Parallelism settings manage the number of concurrent
workers to prevent exceeding API limits.

Constants:
    RATE_LIMIT_DELAY (float): Delay in seconds between API requests.
    MAX_RETRIES (int): Maximum number of retries for 429 Too Many Requests errors.
    RETRY_BASE_DELAY (int): Base delay in seconds for exponential backoff retries.
    MAX_WORKERS (int): Maximum number of concurrent workers to avoid rate limiting.
    LABEL_REMOVAL_DELAY (float): Delay in seconds after label removal operations.
"""

# Rate limiting configuration for API requests
RATE_LIMIT_DELAY = 0.5  # Delay in seconds between API requests
MAX_RETRIES = 3  # Maximum number of retries for 429 errors
RETRY_BASE_DELAY = 10  # Base delay in seconds before retry (exponential backoff)

# Parallelism configuration
MAX_WORKERS = 5  # Reduced from 10 to avoid rate limiting

# Specific delays for different operations
LABEL_REMOVAL_DELAY = 0.1  # Delay after label removal operations