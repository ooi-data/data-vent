import requests


def check_in_progress(status_url):
    """Look for the existance of the status.txt"""
    r = requests.get(status_url)
    if r.status_code == 200:
        return False
    return True
