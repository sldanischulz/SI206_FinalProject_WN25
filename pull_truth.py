import http.client
import re
import os
import requests
import json
import sqlite3
from truthbrush.api import Api
from datetime import datetime, timezone, timedelta
import json
from polygon import RESTClient
from datetime import datetime
import jwt
from cryptography.hazmat.primitives import serialization
import time
import secrets

# Truth Social API Request
def truth_user_lookup(api, handle):
    """Inputs: One API Package, One User Handle (str)
       Output: Returns the data pulled from the API and creates json
       Desc: Pulls a user's basic information from Truthsocial and stores it in a .json file"""


    user = api.lookup(handle)
    with open(f"{handle}_info.json", "w") as json_file:
        json.dump(user, json_file, indent=4)
    return user

def truth_pull_posts(api, handle, start_date):
    
    """Inputs: One API Package, One User Handle (str), one start date (str or datetime obj in yyy-mm-dd format)
       Output: Returns the pulled posts in a dictionary, and creates json
       Desc: Pulls a user's posts from Truthsocial and stores it in a .json file. Pulls posts from most recent to end date specified"""
    
    partial_pull = []
    # partial_pull = list(api.pull_statuses(username=handle, replies=False, created_after=start_date, verbose=True))
    try:
        for a in (api.pull_statuses(username=handle, replies=False, created_after=start_date, verbose=True)):
            # print(a)
            partial_pull.append(a)
            
    except:
        print(f"Truthbrush is done early before the date. The last post date is {partial_pull[-1]['created_at'][0:10]}")

    # print(partial_pull)
    with open(f"{str(start_date)[0:9]}_statuses.json", "w") as json_file:
        json.dump(partial_pull, json_file, indent=4)
    return partial_pull

offset = timezone(timedelta(hours=2))
dt_start = datetime(2024, 11, 1, tzinfo=offset)
dt_end = datetime(2025, 4, 4, tzinfo=offset)

truth_pull_posts(Api(), "realdonaldtrump", dt_start)