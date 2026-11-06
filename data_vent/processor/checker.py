import requests

# OOI occasionally stalls rather than refusing; without this a readiness check
# can block the task indefinitely.
STATUS_TIMEOUT = 60


def check_in_progress(status_url):
    """Look for the existance of the status.txt

    OOI writes status.txt only once an async request finishes, so 404 means the
    request is still running. Anything else is a real error and is raised so the
    caller's retries can handle it, rather than being read as "still waiting".
    """
    r = requests.get(status_url, timeout=STATUS_TIMEOUT)
    if r.status_code == 200:
        return False
    if r.status_code == 404:
        return True
    r.raise_for_status()
    return True
