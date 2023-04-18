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

def get_by_tweet_id(str_input):
    db=connect()
    r=list(db.tweets.find({"post_id":str_input}))
    return r


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
    return list

#get_hashtags("corona")

def get_tweets(str_input):
    list1=[]
    list2=[]
    q_list=[]
    db=connect()
    tot1=0
    strt=time.time()
    results=db.tweets.find({"tweet":{"$regex": "(?i){}".format(" "+str_input+" ")}}).hint("priority_-1")
    end=time.time()
    for i in results:
        i["priority0"]=1
        list1.append(i)
        print(i["tweet"])
        print(i["priority"])
        print("-------------------x-----------------------")
    #print(len(list))
    tot1=end-strt
    #print(f"Total Time={end-strt:.5f}")+
    #####
    ####
    #now searching by words
    pattern=".*(?i)"
    words=str_input.split()
    tot2=0
    if(len(words)>1):
        for i in words:
            pattern+=i+".*(?i)"
        pattern=pattern[:-4]
        strt=time.time()
        results=db.tweets.find({"tweet":{"$regex": pattern}}).hint("priority_-1")
        result = db.test.find({
        "$and":[
                {"tweet": {"$regex": pattern}},
                {"tweet": {"$not" :{"$regex": "(?i){}".format(" "+str_input+" ")}}}]})
        end=time.time()
        for i in results:
            i["priority0"]=0
            list2.append(i)
            print(i["tweet"])
            print(i["priority"])
            print("-------------------x-----------------------")
        list=list1+list2
        print(len(list))
        tot2+=end-strt
    for i in list:
        if(not i["quoted_id"]=="NULL"):
            q_list.append(get_by_tweet_id(i["quoted_id"]))
    print(f"Total Time={tot1+tot2:.5f}")
    return list1,list2,q_list

l1,l2,q_re=get_tweets("covid 19")

def get_by_user(str_input):
    list=[]
    db=connect()
    strt=time.time()
    r=db.tweets.find({"user_id":str_input})
    end=time.time()
    for i in r:
        list.append(i)
        print(i["tweet"])
        print("-------------------x-----------------------")
    print(len(list))
    print(f"Total Time={end-strt:.5f}")
    return list

#get_by_user("1087735689091928064")

def get_retweets(str_input):
    list=[]
    db=connect()
    strt=time.time()
    r=db.retweets.find({"main_tweet_id":str_input}).sort("retweets_count",1)
    end=time.time()
    for i in r:
        list.append(i)
        print(i["user_id"])
        print("Likes=",i["Likes"])
        print("-------------------x-----------------------")
    print(len(list))
    print(f"Total Time={end-strt:.5f}")
    return list

#get_retweets("1249315454797168641")

ids=[]
j=0
def Intersection(lst1, lst2):
    return [d1 for d1 in lst1 for d2 in lst2 if d1 == d2]
print(Intersection(l1,l2))
#print(len(re1))
#print(len(q_re))
# for i in re:
#     ids.append(i["post_id"])
#     if(i["post_id"] in ids):
#         print(i["tweet"])
#         print("------------------")
#         j=j+1
# print(j)