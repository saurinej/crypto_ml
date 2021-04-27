import time
import pandas as pd
import crypto_config
import mysql.connector
from mysql.connector import errorcode

# When will it be used and how?
# Routinely and automated
# Gather data from the crypto database and prep it for training
# Get parameters for the training data
# Parameters - days of data, TA, percentage of growth over how long
# how many points for each training example

# Flow of program:
# Get parameters to set up training data (TA indicator, number of values to calculate test value)
# Use parameters to pull data from database
# Remove 'timestamp' column from data
# Add TA indicators from ta library to data frame
# Remove incomplete rows at the beginning of data frame (approx. 99)
# Adjust values to be within 0 and 1
# Calculate test values (start from end of first data point, maybe 10 per training example??)
# Remove rows without test values at the end of the database (approx. 12(5m)/4(15m), 45min)
# Test values are whether it moves up 1% or not from (OHLC)??
# Split up data into individual test examples
# Make sure training data and test data match up
# Return data


def get_data_5m(coin_pair):
    # Open up mysql connection
    try:
        conn = mysql.connector.connect(user=crypto_config.mysql['user'], password=crypto_config.mysql['password'],
                                       host=crypto_config.mysql['server'], database=crypto_config.mysql['database'])
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
    query = "SELECT cid FROM crypto WHERE coin_abbr={0} AND market={1}".format(coin_pair.split("/"))
    cursor.execute(query)
    cid = cursor.fetchone()

    current_unix_ms = time.time() * 1000
    query = 'SELECT unix_time, c_open, high, low, c_close, volume from ohlcv_5 where cid={0}'.format(cid)
    data_5m = pd.read_sql(query, conn)


