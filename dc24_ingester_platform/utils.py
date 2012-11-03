"""
"""

import time
import datetime
import email.utils as eut

class FixedOffset(datetime.tzinfo):
    """Fixed offset in minutes east from UTC."""

    def __init__(self, offset, name):
        self.__offset = datetime.timedelta(minutes = offset)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return datetime.timedelta(0)

UTC = FixedOffset(0, "UTC")


def format_timestamp(in_date):
    if type(in_date) == str:
        in_date = datetime.datetime(*eut.parsedate(in_date)[:6])
    r = in_date.strftime("%Y-%m-%dT%H:%M:%S.%f")
    r = r[0:r.find(".")+4] + 'Z'
    return r

def parse_timestamp(date_str):
    """Parse the date time returned by the DAM"""
    (dt, mSecs) = date_str.strip().split(".") 
    if mSecs.endswith('Z'): mSecs = mSecs[:-1]
    mSecs = mSecs+'0'*(6-len(mSecs))
    dt = datetime.datetime(*time.strptime(dt, "%Y-%m-%dT%H:%M:%S")[0:6], tzinfo=UTC)
    mSeconds = datetime.timedelta(microseconds = int(mSecs))

    return dt+mSeconds
