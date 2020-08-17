import pandas as pd
import re
import datetime
import pytz


def sql_from_file(file_path):
    """Return sql query form a filepath.

    Arguments:
        file_path {str} -- relative or absolute file path to sql query file. preferred file extension is .sql.

    Returns:
        str -- text of sql 

    """
    with open(file_path, mode='r') as file:
        sql_script = file.read()
    return sql_script


def sql_get_params(script):
    """Return paramters of bigquery sql script.

    Arguments:
        script {str} -- series of sql statements delimited by semi-colon ";", where parameters are declared at top and last statement is the main sql statement

    Returns:
        list of str -- query parameters declared at top of script
    """
    regex = re.compile('\s*DECLARE ([A-Za-z0-9_]*)', re.IGNORECASE)
    lines = script.split(';')
    return [re.findall(regex, param)[0] for param in lines if re.search(regex, param)]


def sql_parameterize(script, params):
    """Inject script parameters values into bigquery sql script.

    Arguments:
        script {str} -- series of sql statements delimited by semi-colon ";", where parameters are declared at top and last statement is the main sql statement
        params {dict} -- parameter : values for the script

    Returns:
        str -- main sql statement with parameter values

    Example:
    script=sql_from_file('./sample_super_store.sql') # read script from file
    sql_get_params(script) # list of parameters in script
    query_params = {'startDate' : f"'{startDate}'", 'endDate' : f"'{endDate}'" , 'latest_reportingMonth' : f"'{latest_reportingMonth}'"} # create dict for parameter values

    sql = sql_parameterize(script, query_params) # inserts parameter values into script
    print(sql)
    client.query(sql).to_dataframe() # get result as dataframe
    """
    query = script.split(';')[-1]

    substrings = sorted(params, key=len, reverse=True)
    regex = re.compile('|'.join(map(re.escape, substrings)))
    return regex.sub(lambda match: params[match.group(0)], query)


def reportingMonthCurrent():
    """Return current month in yyyymm format from local system time as string."""
    return datetime.datetime.now().strftime("%Y%m")


def reportingMonth(cutoffDay=5):
    """Return lastest closed reporting month (usually previous month) in yyyymm format from local system time.

    Arguments:
        cutoffDay {int} -- day when reports are closed (default: {5}), negative cutoffDay number returns current month
        if current day of month is after cutoffday i.e. reports are closed, reporting month is previous month
        if current day of month is before cutoffday i.e. reports are open, reporting month is 2 months before current month

    Returns:
        {str} -- in yyyymm format from local system time
    """
    currentday = datetime.datetime.now()

    if cutoffDay <= 0:
        DD = -datetime.timedelta(days=0)
    elif currentday.day < cutoffDay:
        # can't handle leap year if run on 1st March returns 202012 instead of 202001
        DD = datetime.timedelta(days=60)
    else:
        # can't handle leap year if run on 1st March returns 202001 instead of 202002
        DD = datetime.timedelta(days=30)
    latestMonth = currentday - DD
    return latestMonth.strftime("%Y%m")


def reportingMonthOffset(num_mon=6, reportingMonth=None):
    """Return offset of current month in yyyymm format from local system time. Use this function to get beginning reporting month.

    Arguments:
        num_mon {int} -- number of months to offset from current month (default: 6)
        reportingMonth {str} -- base reportingMonth to offset from (default: current month)

    Returns:
        {str} -- in yyyymm format from local system time

    """
    if not reportingMonth:
        reportingMonth = datetime.datetime.now().strftime("%Y%m")

    date = datetime.datetime.strptime(reportingMonth, "%Y%m").date()
    delta = datetime.timedelta(days=num_mon*30)
    offsetdate = date - delta
    return offsetdate.strftime("%Y%m")


def reportingMonthEnd(reportingMonth=None):
    """Return reportingMonth end date as datetime.date object.

    Arguments:
        reportingMonth {str} -- in yyyymm format (default: current month)

    Returns:
        {datetime.date} -- last day of reportingMonth

    """
    from calendar import monthrange  # infrequent import

    if not reportingMonth:
        reportingMonth = datetime.datetime.now().strftime("%Y%m")
    date = datetime.datetime.strptime(reportingMonth, "%Y%m")
    return datetime.datetime.strptime(f"{reportingMonth}{monthrange(date.year, date.month)[1]}", "%Y%m%d").date()


def scanDate(num_days=45):
    """Return string representation of date in yyyy-mm-dd format by offsetting current date.Use this function to limit amount of data scanned in bigquery date partitioned table.

    Keyword Arguments:
        num_days {int} -- numbers of days to offset current date (default: 45)

    Returns:
        {str} -- representation of date in yyyy-mm-dd format

    """
    sdate = datetime.datetime.now(tz=pytz.timezone(
        'America/New_York')) - datetime.timedelta(days=num_days)
    return sdate.strftime("%Y-%m-%d")


def datetime_fucs(date_time=None, timezone='UTC', truncate_to=None, output_option=None):
    """Return current timestamp, add/translate timezone, formats datetime and converts datetime to UNIX timestamp and int/float to datetime.Reference code for working with datetime objects.

    Keyword Arguments:
        date_time {datetime.datetime or int or float} -- for conversion to UNIX supply datetime, for conversion to datetime supply int or float (default: {None})
        timezone {str} -- pytz.timezone name e.g. (default: {'UTC'})
        truncate_to {str} -- {'second','day'} changes datetime resolution to specified unit (default: {None})
        output_option {str} -- {'UNIX','DATETIME'} for converstion between UNIX and datetime.datetime (default: {None})

    Examples:
    utc_now = datetime_fucs() # utc timestamp

    est_now = datetime_fucs(date_time=utc_now, timezone='America/New_York') # translate time_zone

    datetime_fucs(date_time=est_now, truncate_to='second').isoformat() # adjust resolution

    datetime_fucs(truncate_to='second' ,output_option='UNIX') # unix timestamp to second resolution

    datetime_fucs(truncate_to='second' ,output_option='UNIX') # unix timestamp to second resolution

    # return trip:
    utc_now = datetime_fucs() # datetime
    unix_now = datetime_fucs(date_time=utc_now, output_option='UNIX')  # float
    rtm = datetime_fucs(date_time=unix_now,output_option='DATETIME') # back to datetime
    utc_now  - rtm # timedelta => 0

    """
    if not date_time:
        date_time = datetime.datetime.now(pytz.timezone(timezone))
        if timezone == 'UTC' and truncate_to == None and output_option == None:
            return date_time

    if timezone != 'UTC' and isinstance(date_time, datetime.datetime):
        date_time = date_time.replace(tzinfo=pytz.timezone(timezone))

        if truncate_to == None and output_option == None:
            return date_time
    else:
        timezone = pytz.timezone(timezone)

    if truncate_to:
        if truncate_to == 'second':
            date_time = date_time.replace(microsecond=0)
        else:  # truncate_to == 'day'
            date_time = datetime.date(
                date_time.year, date_time.month, date_time.day)

    if output_option:
        if output_option == 'UNIX':
            if isinstance(date_time, datetime.datetime):
                return date_time.timestamp()
            elif str(type(date_time)) == "<class 'pandas._libs.tslibs.timestamps.Timestamp'>":
                #                 print("pd.Timestamp class")
                return (date_time - pd.Timestamp("1970-01-01", tz=new_timezone)) // pd.Timedelta('1s')
            else:
                #                 print('assume datetime.date')
                return time.mktime(date_time.timetuple())

        elif output_option == 'DATETIME':
            if isinstance(date_time, int) or isinstance(date_time, float):
                #                 print('unix to datetime')
                return datetime.datetime.fromtimestamp(date_time, tz=timezone)
            else:
                return pd.to_datetime(date_time, unit='s', origin='unix')
    else:
        return date_time
