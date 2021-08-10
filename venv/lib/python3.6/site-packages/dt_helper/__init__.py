import pytz
import settings_helper as sh
import input_helper as ih
from datetime import datetime, time, timedelta, timezone as dt_timezone
from functools import partial
from itertools import product, zip_longest, chain


get_setting = sh.settings_getter(__name__)
FLOAT_STRING_FMT = '%Y%m%d%H%M%S.%f'
ADMIN_TIMEZONE = get_setting('admin_timezone')
ADMIN_DATE_FMT = get_setting('admin_date_fmt')
SECONDS_IN_HOUR = 60 * 60
SECONDS_IN_DAY = SECONDS_IN_HOUR * 24
SECONDS_IN_WEEK = SECONDS_IN_DAY * 7


def seconds_to_duration(seconds):
    """Return a string representing the number of seconds as a duration"""
    parts = []
    weeks, seconds = divmod(seconds, SECONDS_IN_WEEK)
    if weeks > 0:
        part = '{} '.format(int(weeks))
        part += 'week' if weeks == 1 else 'weeks'
        parts.append(part)
    days, seconds = divmod(seconds, SECONDS_IN_DAY)
    if days > 0:
        part = '{} '.format(int(days))
        part += 'day' if days == 1 else 'days'
        parts.append(part)
    hours, seconds = divmod(seconds, SECONDS_IN_HOUR)
    if hours > 0:
        part = '{} '.format(int(hours))
        part += 'hour' if hours == 1 else 'hours'
        parts.append(part)
    minutes, seconds = divmod(seconds, 60)
    if minutes > 0:
        part = '{} '.format(int(minutes))
        part += 'minute' if minutes == 1 else 'minutes'
        parts.append(part)
    if seconds > 0:
        part = '{} '.format(seconds)
        part += 'second' if seconds == 1 else 'seconds'
        parts.append(part)
    return ', '.join(parts)


def utc_now_localized():
    """Return a localized datetime object for current UTC time"""
    return pytz.utc.localize(datetime.utcnow())


def utc_now_iso():
    """Return current UTC timestamp in ISO format"""
    return datetime.utcnow().isoformat()


def days_ago(days=0, timezone="America/Chicago"):
    """Return datetime object representing UTC start of day for timezone"""
    days = days if days >= 0 else 0
    tz = pytz.timezone(timezone)
    today = utc_now_localized().astimezone(tz).date()
    dt = tz.localize(datetime.combine(today - timedelta(days=days), time()))
    return dt.astimezone(pytz.utc)


def dt_to_float_string(dt, fmt=FLOAT_STRING_FMT):
    """Return string representation of a utc_float from given dt object"""
    s = dt.strftime(fmt)
    result = ih.from_string(s)
    if type(result) != str:
        result = repr(result)
    return result


def float_string_to_dt(float_string, fmt=FLOAT_STRING_FMT):
    """Return a dt object from a utc_float"""
    float_string = str(float_string)
    if '.' not in float_string:
        float_string = float_string + '.0'
    return datetime.strptime(str(float_string), fmt)


def local_now_string(fmt=FLOAT_STRING_FMT):
    """Return string representation of local time right now"""
    return dt_to_float_string(datetime.now(), fmt)


def utc_now_float_string(fmt=FLOAT_STRING_FMT):
    """Return string representation of a utc_float for right now"""
    return dt_to_float_string(datetime.utcnow(), fmt)


def utc_ago_float_string(num_unit, now=None, fmt=FLOAT_STRING_FMT):
    """Return a float_string representing a UTC datetime in the past

    - num_unit: a string 'num:unit' (i.e. 15:seconds, 1.5:weeks, etc)
    - now: a utc_float or None

    Valid units are: (se)conds, (mi)nutes, (ho)urs, (da)ys, (we)eks, hr, wk
    """
    if now is None:
        now = datetime.utcnow()
    else:
        now = float_string_to_dt(now)
    val = None
    num, unit = num_unit.split(':')
    _trans = {
        'se': 'seconds', 'mi': 'minutes', 'ho': 'hours', 'hr': 'hours',
        'da': 'days', 'we': 'weeks', 'wk': 'weeks'
    }
    try:
        kwargs = {_trans[unit.lower()[:2]]: float(num)}
    except (KeyError, ValueError) as e:
        pass
    else:
        td = timedelta(**kwargs)
        val = dt_to_float_string(now - td, fmt)
    return val


def utc_float_to_pretty(utc_float=None, fmt=None, timezone=None):
    """Return the formatted version of utc_float

    - fmt: a strftime format
    - timezone: a timezone

    If no utc_float is provided, a utc_float for "right now" will be used. If no
    fmt is provided and admin_date_fmt is in settings.ini, settings will be used
    """
    if not utc_float:
        utc_float = float(utc_now_float_string())
    elif type(utc_float) == str and '.' not in utc_float:
        utc_float = float(utc_float + '.0')
    if not fmt:
        if ADMIN_DATE_FMT:
            fmt = ADMIN_DATE_FMT
            timezone = ADMIN_TIMEZONE
        else:
            return utc_float
    dt = datetime.strptime(str(utc_float), FLOAT_STRING_FMT)
    if timezone:
        dt = dt.replace(tzinfo=dt_timezone.utc)
        dt = dt.astimezone(pytz.timezone(timezone))
    return dt.strftime(fmt)


def date_string_to_utc_float_string(date_string, timezone=None):
    """Return a utc_float_string for a given date_string

    - date_string: string form between 'YYYY' and 'YYYY-MM-DD HH:MM:SS.f'
    """
    dt = None
    s = None
    for fmt in [
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d %H', '%Y-%m-%d', '%Y-%m', '%Y'
    ]:
        try:
            dt = datetime.strptime(str(date_string), fmt)
        except ValueError:
            continue
        else:
            break

    if dt:
        if timezone:
            tz = pytz.timezone(timezone)
            dt = tz.localize(dt).astimezone(pytz.utc)
        s = dt_to_float_string(dt)
    return s


def date_string_to_datetime(date_string, fmt='%Y-%m-%d', timezone=None):
    """Return a date object for a string of a given format."""
    if isinstance(date_string, datetime):
        return date_string
    try:
        dt = datetime.strptime(date_string, fmt)
    except ValueError:
        # Truncate fractional seconds from string if not included in fmt
        dt = datetime.strptime(date_string.split('.')[0], fmt)

    if timezone:
        tz = pytz.timezone(timezone)
        return tz.localize(dt)
    return dt


def date_start_utc(date_string, fmt='%Y-%m-%d', timezone="America/Chicago"):
    """Return datetime object representing UTC start of day for timezone on date"""
    dt = date_string_to_datetime(date_string, fmt=fmt, timezone=timezone)
    return dt.astimezone(pytz.utc)


def get_time_ranges_and_args(**kwargs):
    """Return a dict of time range strings and start/end tuples

    Multiple values in (start_ts, end_ts, since, until) must be separated
    by any of , ; |

    - tz: timezone
    - now: float_string
    - start: utc_float
    - end: utc_float
    - start_ts: timestamps with form between YYYY and YYYY-MM-DD HH:MM:SS.f (in tz)
    - end_ts: timestamps with form between YYYY and YYYY-MM-DD HH:MM:SS.f (in tz)
    - since: 'num:unit' strings (i.e. 15:seconds, 1.5:weeks, etc)
    - until: 'num:unit' strings (i.e. 15:seconds, 1.5:weeks, etc)

    The start/end kwargs returned are meant to be used with any of
    REDIS functions zcount, zrangebyscore, or zrevrangebyscore
    """
    tz = kwargs.get('tz') or ADMIN_TIMEZONE
    now = kwargs.get('now') or utc_now_float_string()
    results = {}
    _valid_args = [
        ('start_ts', 'end_ts', partial(date_string_to_utc_float_string, timezone=tz)),
        ('since', 'until', partial(utc_ago_float_string, now=now)),
    ]
    for first, second, func in _valid_args:
        first_string = kwargs.get(first, '')
        second_string = kwargs.get(second, '')
        if first_string or second_string:
            first_vals = ih.string_to_set(first_string)
            second_vals = ih.string_to_set(second_string)
            if first_vals and second_vals:
                _gen = product(first_vals, second_vals)
                gen = chain(
                    _gen,
                    ((f, '') for f in first_vals),
                    (('', s) for s in second_vals)
                )
            else:
                gen = zip_longest(first_vals, second_vals)

            for _first, _second in gen:
                if _first and _second:
                    return_key = '{}={},{}={}'.format(first, _first, second, _second)
                    start_float = float(func(_first))
                    end_float = float(func(_second))
                elif _first:
                    return_key = '{}={}'.format(first, _first)
                    start_float = float(func(_first))
                    end_float = float('inf')
                elif _second:
                    return_key = '{}={}'.format(second, _second)
                    end_float = float(func(_second))
                    start_float = 0
                else:
                    continue
                if start_float >= end_float:
                    continue

                results[return_key] = (start_float, end_float)

    start = kwargs.get('start')
    end = kwargs.get('end')
    if start and end:
        return_key = 'start={},end={}'.format(start, end)
        results[return_key] = (float(start), float(end))
    elif start:
        return_key = 'start={}'.format(start)
        results[return_key] = (float(start), float('inf'))
    elif end:
        return_key = 'end={}'.format(end)
        results[return_key] = (0, float(end))
    if not results:
        results['all'] = (0, float('inf'))
    return results


def get_timestamp_formatter_from_args(ts_fmt=None, ts_tz=None, admin_fmt=False):
    """Return a function that can be applied to a utc_float

    - ts_fmt: strftime format for the returned timestamp
    - ts_tz: a timezone to convert the timestamp to before formatting
    - admin_fmt: if True, use format and timezone defined in settings file
    """
    if admin_fmt:
        func = partial(
            utc_float_to_pretty, fmt=ADMIN_DATE_FMT, timezone=ADMIN_TIMEZONE
        )
    elif ts_tz and ts_fmt:
        func = partial(utc_float_to_pretty, fmt=ts_fmt, timezone=ts_tz)
    elif ts_fmt:
        func = partial(utc_float_to_pretty, fmt=ts_fmt)
    else:
        func = lambda x: x
    return func
