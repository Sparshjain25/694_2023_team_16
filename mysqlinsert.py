import csv
import sqlite3
import pandas as pd
import json
import mysql.connector

# establish connection to database
cnx = mysql.connector.connect(user='root', 
                              password='Mayank@45', 
                              host='localhost',
                              database='mydatabase',
                              auth_plugin = 'mysql_native_password')

cursor = cnx.cursor()

# create tables if they do not already exist
cursor.execute(" CREATE TABLE IF NOT EXISTS user (\
  id VARCHAR(255) PRIMARY KEY,\
  name VARCHAR(255),\
  screen_name VARCHAR(255),\
  location VARCHAR(255),\
  description TEXT,\
  followers_count INT,\
  friends_count INT,\
  favourites_count INT,\
  statuses_count INT,\
  verified BOOLEAN,\
  created_at VARCHAR(255),\
  url VARCHAR(255) )")


with open("/Users/mayank/Downloads/corona_out_2.txt", "r") as f1:
    for line in f1:
        try:
            data = json.loads(line)
            if (data['text'].startswith('RT')):
                #adding retweet in user details
                cursor.execute("SELECT * FROM user WHERE id = %s", (data['user']['id_str'],))
                if (cursor.fetchone() is None):
                    user = data['user']
                    user_id = user['id_str']
                    user_values = (user_id, user['name'], user['screen_name'], user['location'], user['description'], 
                     user['followers_count'], user['friends_count'], user['favourites_count'], user['statuses_count'], 
                     user['verified'], user['created_at'], user['url'])
                    cursor.execute("INSERT INTO user (id, name, screen_name, location, description, followers_count, "
                               "friends_count, favourites_count, statuses_count, verified, created_at, url) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", user_values)

                
                #adding quoted tweet
                if (data['is_quote_status']):
                    cursor.execute("SELECT * FROM user WHERE id = %s", (data['quoted_status']['user']['id_str'],))
                    if (cursor.fetchone() is None):
                        user = data['quoted_status']['user']
                        user_id = user['id_str']
                        user_values = (user_id, user['name'], user['screen_name'], user['location'], user['description'], 
                         user['followers_count'], user['friends_count'], user['favourites_count'], user['statuses_count'], 
                         user['verified'], user['created_at'], user['url'])
                        cursor.execute("INSERT INTO user (id, name, screen_name, location, description, followers_count, "
                                   "friends_count, favourites_count, statuses_count, verified, created_at, url) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", user_values)

                
                #adding retweeted tweet user details
                cursor.execute("SELECT * FROM user WHERE id = %s", (data['retweeted_status']['user']['id_str'],))
                if (cursor.fetchone() is None):
                    user = data['retweeted_status']['user']
                    user_id = user['id_str']
                    user_values = (user_id, user['name'], user['screen_name'], user['location'], user['description'], 
                     user['followers_count'], user['friends_count'], user['favourites_count'], user['statuses_count'], 
                     user['verified'], user['created_at'], user['url'])
                    cursor.execute("INSERT INTO user (id, name, screen_name, location, description, followers_count, "
                               "friends_count, favourites_count, statuses_count, verified, created_at, url) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", user_values)

                    
            else:
                 #adding quoted tweet
                if (data['is_quote_status']):    #check if tweet is quoted
                    cursor.execute("SELECT * FROM user WHERE id = %s", (data['quoted_status']['user']['id_str'],))
                    if (cursor.fetchone() is None):
                        user = data['quoted_status']['user']
                        user_id = user['id_str']
                        user_values = (user_id, user['name'], user['screen_name'], user['location'], user['description'], 
                         user['followers_count'], user['friends_count'], user['favourites_count'], user['statuses_count'], 
                         user['verified'], user['created_at'], user['url'])
                        cursor.execute("INSERT INTO user (id, name, screen_name, location, description, followers_count, "
                                   "friends_count, favourites_count, statuses_count, verified, created_at, url) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", user_values)

                        
                #adding retweeted tweet user details
                cursor.execute("SELECT * FROM user WHERE id = %s", (data['user']['id_str'],))
                if (cursor.fetchone() is None):
                    user = data['user']
                    user_id = user['id_str']
                    user_values = (user_id, user['name'], user['screen_name'], user['location'], user['description'], 
                     user['followers_count'], user['friends_count'], user['favourites_count'], user['statuses_count'], 
                     user['verified'], user['created_at'], user['url'])
                    cursor.execute("INSERT INTO user (id, name, screen_name, location, description, followers_count, "
                               "friends_count, favourites_count, statuses_count, verified, created_at, url) "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", user_values)

        except:
            continue

cnx.commit()
