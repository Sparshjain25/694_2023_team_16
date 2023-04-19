import re
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

def order_tweets(words,list):
    for i in list:
        pattern = re.compile(re.escape(words[0]), re.IGNORECASE | re.DOTALL)
        strt=pattern.search(i["tweet"]).span()[0]+len(words[0])-1
        dist=0
        l=i["tweet"][strt:]
        for j in words[1:]:
            pattern=re.compile(re.escape(j), re.IGNORECASE | re.DOTALL)
            dist=dist+pattern.search(l).span()[0]-1
            strt=pattern.search(i["tweet"]).span()[0]+len(j)-1
            l=l[strt:]
        i["Dist"]=dist
    list= sorted(list, key=lambda x: x['Dist'])
    return list

def get_by_tweet_id(str_input):
    db=connect()
    r=list(db.tweets.find({"post_id":str_input}))
    return r

def get_hashtags(str_input):
    lists=[]
    str_input=str_input.replace(" ","")
    db=connect()
    strt=time.time()
    lists=list(db.tweets.find({"hashtags":{"$regex": "(?i){}".format(str_input)}}).hint('priority_-1'))
    end=time.time()
    # for i in results:
    #     list.append(i)
    for i in range(20):
        print(lists[i]["hashtags"])
        print(lists[i]["priority"])
        print("__________________")
    print()
    print(len(lists))
    print(f"Total Time={end-strt:.5f}")
    return lists

#get_hashtags("corona")

def get_tweets(str_input):
    list1=[]
    list2=[]
    q_list=[]
    db=connect()
    ##FULL MATCH
    tot1=0
    strt=time.time()
    results=db.tweets.find({"tweet":{"$regex": "(?i){}".format(" "+str_input+" ")}}).hint("priority_-1")
    end=time.time()
    for i in results:
        i["Dist"]=-1
        list1.append(i)
        print("tweet=",i["tweet"])
        print("priority",i["priority"])
        print("Dist=",i["Dist"])
        print("-------------------x-----------------------")
    tot1=end-strt
    #now searching by words  --- Partial Match
    pattern=".*(?i)"
    words=str_input.split()
    tot2=0
    if(len(words)>1):
        for i in words:
            pattern+=i+".*(?i)"
        pattern=pattern[:-4]
        strt=time.time()
        result = db.tweets.find({
            "$and":[
                {"tweet": {"$regex": pattern}},
                {"tweet": {"$not" :{"$regex": "(?i){}".format(" "+str_input+" ")}}}]}).hint("priority_-1")
        end=time.time()
        for i in result:
            list2.append(i)
        list2=order_tweets(words,list2)
        for i in range(20):
            print(list2[i]["tweet"])
            print("P=",list2[i]["priority"])
            print("Dist=",list2[i]["Dist"])
            print("ITERATION============",i)
            print("________________________________________")
        list=list1+list2
        print(len(list))
        tot2+=end-strt
    for i in list:
        if(not i["quoted_id"]=="NULL"):
            q_list.append(get_by_tweet_id(i["quoted_id"]))
    print(f"Total Time={tot1+tot2:.5f}")
    return list,q_list

#l1,l2,q_re=get_tweets("corona 19")

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
    r=db.retweets.find({"main_tweet_id":str_input})#.sort("retweets_count",1)
    end=time.time()
    for i in r:
        list.append(i)
        print(i["user_name"])
        print("Likes=",i["Likes"])
        print("-------------------x-----------------------")
    print(len(list))
    print(f"Total Time={end-strt:.9f}")
    return list

#get_retweets("1249315454797168641")
