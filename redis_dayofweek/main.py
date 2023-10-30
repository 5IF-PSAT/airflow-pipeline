# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import os
from datetime import datetime, timedelta
import redis

HOST = os.environ.get('REDIS_HOST', 'redis')
PORT = os.environ.get('REDIS_PORT', 6379)
DB = os.environ.get('REDIS_DB', 0)

if __name__ == '__main__':
    start_date = datetime(2017, 1, 1)
    end_date = datetime(2022, 12, 31)
    r = redis.Redis(host=HOST, port=PORT, db=DB)

    while start_date <= end_date:
        if r.get(start_date.strftime("%Y-%m-%d")) is None:
            r.set(start_date.strftime("%Y-%m-%d"), start_date.strftime('%A'))
        start_date += timedelta(days=1)


# See PyCharm help at https://www.jetbrains.com/help/pycharm/
