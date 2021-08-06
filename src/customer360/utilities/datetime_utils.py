import datetime

import pytz


def get_local_datetime():
    """
    Purpose: To ge the local date time values.
    :return:
    """
    return (
        datetime.datetime.utcnow()
        .replace(microsecond=0, tzinfo=pytz.timezone("UTC"))
        .astimezone(tz=pytz.timezone("Asia/Bangkok"))
    )