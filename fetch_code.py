import numpy as np
import json
import pandas
import pymongo
import time
from urllib.parse import quote_plus

def connect():
    username="dbmspro"
    password="dbmspro"
    escaped_username = quote_plus(username)
    escaped_password = quote_plus(password)
    conn=f"mongodb+srv://{escaped_username}:{escaped_password}@cluster1.agzdz5g.mongodb.net/?retryWrites=true&w=majority"   

    client = pymongo.MongoClient(conn)
    db=client.dbmspro
    return db

def get_hashtags(str_input):
    list=[]
    str_input=str_input.replace(" ","")
    db=connect()
    strt=time.time()
    results=db.tweets.find({"hashtags":{"$regex": "(?i){}".format(str_input)}})
    end=time.time()
    for i in results:
        list.append(i)
        print(i["hashtags"])
    print()
    print(len(list))
    print(f"Total Time={end-strt:.5f}")

def get_tweets(str_input):
    list=[]
    db=connect()
    strt=time.time()
    results=db.tweets.find({"tweet":{"$regex": "(?i){}".format(str_input)}})
    end=time.time()
    for i in results:
        i["priority"]=1
        list.append(i)
        print(i["tweet"])
        print("-------------------x-----------------------")
    #print(len(list))
    tot1=end-strt
    #print(f"Total Time={end-strt:.5f}")
    #now searching by words
    pattern=".*"
    words=str_input.split()
    tot2=0
    if(len(words)>1):
        for i in words:
            pattern+=i+".*"
        strt=time.time()
        results=db.tweets.find({"tweet":{"$regex": pattern}})
        end=time.time()
        for i in results:
            i["priority"]=0.5
            list.append(i)
            print(i["tweet"])
            print("-------------------x-----------------------")
        print(len(list))
        tot2+=end-strt
    print(f"Total Time={tot1+tot2:.5f}")

get_tweets("covid 19")


