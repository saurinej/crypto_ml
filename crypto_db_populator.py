import smtplib
import ssl
import time

import mysql.connector
from mysql.connector import errorcode

import crypto_config
import crypto_data_puller

from sqlalchemy import create_engine


# flow of program:
# get all coins names in list
# get cid from database
# check database for last unix time point
# get current unix time point
# create parameters to pull necessary data points based on time points
# get data from data puller module
# remove last data point
# populate database

def populate_database():
    # Get coin pairs
    coin_pairs = []
    for s in crypto_config.coin_pairs:
        coin_pairs.append(s.split('/'))

    user = crypto_config.mysql['user']
    password = crypto_config.mysql['password']
    host = crypto_config.mysql['server']
    database = crypto_config.mysql['database']

    # Open up mysql connection
    try:
        conn = mysql.connector.connect(user=user, password=password, host=host, database=database)
    except mysql.connector.Error as err:
        # Send email using smtplib https://realpython.com/python-send-email/
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        print('Connected successfully')

    # open cursor to execute statements
    cursor = conn.cursor()

    # Add cid to coin pairs
    query = "SELECT cid FROM crypto WHERE coin_abbr=%s AND market=%s"
    cids = []
    for cp in coin_pairs:
        cursor.execute(query, cp)
        cids.append(cursor.fetchone())

    # Create queries to get last unix time for each coin for 5 and 15min intervals
    query_max_ut_5m = "SELECT MAX(unix_time) from ohlcv_5 where cid=%s"
    query_max_ut_15m = "SELECT MAX(unix_time) from ohlcv_15 where cid=%s"

    # Get last unix times for 5min intervals. If none exist, set unix time to 0
    last_unix_5m = []
    for cid in cids:
        cursor.execute(query_max_ut_5m, cid)
        unix = str(cursor.fetchone()).strip("(),")
        unix = 0 if unix == 'None' else int(unix)
        last_unix_5m.append(unix)
    # Get last unix times for 15min intervals. If none exist, set unix time to 0
    last_unix_15m = []
    for cid in cids:
        cursor.execute(query_max_ut_15m, cid)
        unix = str(cursor.fetchone()).strip("(),")
        unix = 0 if unix == 'None' else int(unix)
        last_unix_15m.append(unix)

    # Get current unix time
    current_unix_ms = time.time() * 1000
    current_unix_ms = round(current_unix_ms)

    # Calculate how many intervals to retrieve
    retrieve_points_5m = []
    for last_unix_5 in last_unix_5m:
        if last_unix_5 == 0:
            retrieve_points_5m.append(1000)
        else:
            # + 5 to match up to previous time point
            retrieve_points_5m.append((current_unix_ms - last_unix_5) / (5 * 60 * 1000) + 5)

    retrieve_points_15m = []
    for last_unix_15 in last_unix_15m:
        if last_unix_15 == 0:
            retrieve_points_15m.append(1000)
        else:
            # + 5 to get extra for matching up to previous time point
            retrieve_points_15m.append((current_unix_ms - last_unix_15) / (15 * 60 * 1000) + 5)

    # Put retrieval information into a single list
    retrieval_info_5m = []
    for i in range(len(coin_pairs)):
        retrieval_info_5m.append([coin_pairs[i][0] + "/" + coin_pairs[i][1], 5, retrieve_points_5m[i]])
    retrieval_info_15m = []
    for i in range(len(coin_pairs)):
        retrieval_info_15m.append([coin_pairs[i][0] + "/" + coin_pairs[i][1], 15, retrieve_points_15m[i]])

    # Gather data from crypto_data_puller
    data_5m = []
    data_15m = []
    for cp in retrieval_info_5m:
        df = crypto_data_puller.fetch_data(symbol=cp[0], min_interval=cp[1], points=cp[2])
        time.sleep(1)  # to stay within Binance API limits
        df.drop(df.tail(1).index, inplace=True)
        data_5m.append(df)
    for cp in retrieval_info_15m:
        df = crypto_data_puller.fetch_data(symbol=cp[0], min_interval=cp[1], points=cp[2])
        time.sleep(1)  # to stay within Binance API limits
        df.drop(df.tail(1).index, inplace=True)
        data_15m.append(df)

    # match the first time point of the data gathered to the last unix time pulled from the database
    # three possibilities: 1 - unix times are zero 2 - last time is greater than earliest time pulled
    # (a gap will be present) 3 - times overlap
    # last_unix_5m and last_unix_15m
    # Collect earliest unix time point from collected data for each time interval
    first_unix_5m = []
    first_unix_15m = []
    for i in range(len(data_5m)):
        first_unix_5m.append(data_5m[i]['unix_time'][0])
        first_unix_15m.append(data_15m[i]['unix_time'][0])

    # compare last and earliest time points and adjust data frames
    for i in range(len(first_unix_5m)):
        if last_unix_5m[i] == 0:
            # do nothing, add all data points
            continue
        elif first_unix_5m[i] > last_unix_5m[i]:
            continue
        elif first_unix_5m[i] < last_unix_5m[i]:
            # get index of row that contains unix time then delete that row and rows above
            drop_to = data_5m[i].loc[data_5m[i]['unix_time'] == last_unix_5m[i]].index[0]
            data_5m[i] = data_5m[i].loc(drop_to)
        elif first_unix_5m[i] == last_unix_5m[i]:
            data_5m[i] = data_5m[i].loc(1)
    # Repeat for 15 min interval
    for i in range(len(first_unix_15m)):
        if last_unix_15m[i] == 0:
            # do nothing, add all data points
            continue
        elif first_unix_15m[i] > last_unix_15m[i]:
            # there will be a gap but do nothing
            continue
        elif first_unix_15m[i] < last_unix_15m[i]:
            # get index of row that contains unix time then delete that row and rows above
            drop_to = data_15m[i].loc[data_15m[i]['unix_time'] == last_unix_15m[i]].index[0]
            data_15m[i] = data_15m[i].loc(drop_to)
        elif first_unix_15m[i] == last_unix_15m[i]:
            data_15m[i] = data_15m[i].loc(1)

    # Lastly, prep data for insertion into database
    # Add cid as first column
    for i in range(len(data_5m)):
        data_5m[i].insert(0, 'cid', int(str(cids[i]).strip("(),")))
        data_15m[i].insert(0, 'cid', int(str(cids[i]).strip("(),")))

    # Use sqlAlchemy connection to database, close other connection
    cursor.close()
    conn.close()
    sql_engine = create_engine('mysql+mysqlconnector://' + user + ':' + password + '@' + host + ':3306/' +
                               database + '')
    db_connection = sql_engine.connect()

    for i in range(len(data_5m)):
        data_5m[i].to_sql(name='ohlcv_5', con=db_connection, if_exists='append', index=False)
        data_15m[i].to_sql(name='ohlcv_15', con=db_connection, if_exists='append', index=False)

    # Send an email saying the database update was a success
    # Create a secure SSL context to send an email
    context = ssl.create_default_context()
    # Send email
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(crypto_config.email_info['email_from'], crypto_config.email_info['password'])
        message = """\
        Crypto Database Update - Success

        The crypto database was just updated."""
        server.sendmail(crypto_config.email_info['email_from'], crypto_config.email_info['email_to'], message)
