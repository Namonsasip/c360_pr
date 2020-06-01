import datetime

import pytz


def get_local_datetime():
    return (
        datetime.datetime.utcnow()
        .replace(microsecond=0, tzinfo=pytz.timezone("UTC"))
        .astimezone(tz=pytz.timezone("Asia/Bangkok"))
    )