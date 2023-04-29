import csv
import sqlite3
import pandas as pd
import json
import mysql.connector
import pymysql
from ordered_hash_set import OrderedSet

def connect():
    # establish connection to database
    conn = pymysql.connect(user='root', \
        password='Mayank@45', \
        host='localhost', \
        database='mydatabase')  
    cursor = conn.cursor()

    return cursor

def disconnect(cursor):
    # cursor = connect()
    cursor.close()

def get_users(str_input):
    cursor = connect()
    s = OrderedSet()

    #Query which searches the user account name exactly
    cursor.execute("SELECT * FROM user WHERE name = '%s' \
    ORDER BY verified DESC, followers_count DESC"%str_input)

    res = cursor.fetchall()
    for line in res:
        s.add(line)

    #Query which searches the username exactly
    cursor.execute("SELECT * FROM user WHERE screen_name = '%s' \
    ORDER BY verified DESC, followers_count DESC"%str_input)

    res = cursor.fetchall()
    for line in res:
        s.add(line)

    #Query which searches the user account name partially (starts with)
    qu = "SELECT * FROM user WHERE name LIKE %s\
    ORDER BY verified DESC, followers_count DESC"
    cursor.execute(qu, (str_input + "%",))
    
    res = cursor.fetchall()
    for line in res:
        s.add(line)

    #Query which searches the user username partially (starts with)
    qu = "SELECT * FROM user WHERE name LIKE %s\
    ORDER BY verified DESC, followers_count DESC"
    cursor.execute(qu, (str_input + "%",))
    
    res = cursor.fetchall()
    for line in res:
        s.add(line)

    #Query which searches the user account name partially
    qu = "SELECT * FROM user WHERE name LIKE %s\
    ORDER BY verified DESC, followers_count DESC"
    cursor.execute(qu, ("%" + str_input + "%",))
    
    res = cursor.fetchall()
    for line in res:
        s.add(line)


    #Query which searches the username partially
    lo = str_input.lower()
    qu = ("SELECT * FROM user WHERE screen_name LIKE %s \
    ORDER BY verified DESC, followers_count DESC")
    cursor.execute(qu, ("%" + lo + "%",))

    res = cursor.fetchall()
    for line in res:
        s.add(line)

    for i in s:
        print(i)
    disconnect(cursor)

    return s

def get_user_details(str_input):
    cursor = connect()

    #Query which searches the username exactly
    cursor.execute("SELECT * FROM user WHERE screen_name = '%s' \
    ORDER BY verified DESC, followers_count DESC"%str_input)

    res = cursor.fetchall()
    disconnect(cursor)
    return res
