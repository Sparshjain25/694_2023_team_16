import re
import pymongo
import time
from urllib.parse import quote_plus

class Non_Relational():
    def __init__(self) -> None:
        pass

    def connect(self):
        username="dbmspro"
        password="dbmspro"
        escaped_username = quote_plus(username)
        escaped_password = quote_plus(password)
        conn=f"mongodb+srv://{escaped_username}:{escaped_password}@cluster1.agzdz5g.mongodb.net/?retryWrites=true&w=majority"   

        client = pymongo.MongoClient(conn)
        db=client.dbmspro
        return db

## Ordering is done taking the relevant distance of words present in your query search.
    def order_tweets(self,words,list):
        for i in list:
            pattern = re.compile(re.escape(words[0]), re.IGNORECASE | re.DOTALL)
            strt=pattern.search(i["tweet"]).span()[0]+len(words[0])
            dist=0
            l=i["tweet"][strt:]
            for j in words[1:]:
                pattern=re.compile(re.escape(j), re.IGNORECASE | re.DOTALL)
                dist=dist+pattern.search(l).span()[0]
                strt=pattern.search(l).span()[0]+len(j)
                l=l[strt:]
            i["Dist"]=dist
        list= sorted(list, key=lambda x: x['Dist'])
        return list

## Helps us to get tweets for locating MAIN ID (quoted tweet)
    def get_by_tweet_id(self,str_input):
        db=self.connect()
        r=list(db.tweets_final.find({"post_id":str_input}).hint('post_id_-1'))
        return r

## Ordering is done according to the priority formula
    def get_hashtags(self,str_input):
        str_input=str_input.replace(" ","")
        db=self.connect()
        lists=list(db.tweets_final.find({"hashtags":{"$regex": "(?i){}".format(str_input)}}).hint('priority_-1'))
        return lists

## Helps to get tweets that matches with your query text
    def get_tweets(self,str_input):
        db=self.connect()
        ##FULL MATCH
        list1=list(db.tweets_final.find({"tweet":{"$regex": "(?i){}".format(" "+str_input+" ")}}).hint("priority_-1"))
        #now searching by words  --- Partial Match
        pattern=".*(?i)"
        words=str_input.split()
        for i in words:
            pattern+=i+".*(?i)"
        pattern=pattern[:-4]
        list2 =list(db.tweets_final.find({
            "$and":[
                {"tweet": {"$regex": pattern}},
                {"tweet": {"$not" :{"$regex": "(?i){}".format(" "+str_input+" ")}}}]}).hint("priority_-1"))
        if len(list2) != 0:
            list2=self.order_tweets(words,list2)
        lists=list1+list2
        return lists

## Get tweets of particular user 
    def get_by_user(self,str_input):
        db=self.connect()
        r=list(db.tweets_final.find({"user_id":str_input}).hint('user_id_-1_fmt_time_-1'))
        return r

## Get retweets of particular tweet.
    def get_retweets(self,str_input):

        lists=[]
        db=self.connect()
        lists=list(db.retweets_final.find({"main_tweet_id":str_input}).hint('main_tweet_id_1_fmt_time_-1'))
        return lists

## Get top 20 tweets of all time
    def top_tweets(self):
        db=self.connect()
        r=list(db.tweets_final.find().hint("priority_-1").limit(20))
        return r

## Helps to filter out the tweets by time
    def filter_tweets(self,value,l):
        f=[]
        for i in l:
            if(i["fmt_time"]>=value):
                f.append(i)
        return f

## Get us the timed tweets where time is given by selecting dropdown option
    def get_tweet_by_time(self,input,tr):
        db=self.connect()
        test=Non_Relational()
        r=test.get_tweets(input)
        T=db.tweets_final.find_one({},sort=[("fmt_time",pymongo.DESCENDING)])["fmt_time"]
        if tr=="1":
            i=60*60*24
            T=T-i
            final=test.filter_tweets(T,r)
        elif(tr == "2"):
            i=60*60*24*7
            T=T-i
            final=test.filter_tweets(T,r)
        else:
            i=60*60*24*7*30          #Last month is taken to be for past 30 days
            T=T-i
            final=test.filter_tweets(T,r)
        return final

## Get us the timed tweets searched by hashtags where time is given by selecting dropdown option
    def get_hashtags_by_time(self,input,tr):
        db=self.connect()
        test=Non_Relational()
        r=test.get_hashtags(input)
        T=db.tweets_final.find_one({},sort=[("fmt_time",pymongo.DESCENDING)])["fmt_time"]
        if tr=="1":
            i=60*60*24
            T=T-i
            final=test.filter_tweets(T,r)
        elif(tr == "2"):
            i=60*60*24*7
            T=T-i
            final=test.filter_tweets(T,r)
        else:
            i=60*60*24*7*30          #Last month is taken to be for past 30 days
            T=T-i
            final=test.filter_tweets(T,r)
        return final

## Below are some checks and for practice.

#test=Non_Relational()
#print(test.get_tweet_by_time("corona is here","2"))
#test.get_hashtags("corona")
#print(test.get_hashtags("corona"))
#print(test.get_retweets("1249315454797168641"))
#print(test.get_by_user("1087735689091928064"))
#l1=test.get_tweets("corona is here")
#print(l1)
#print(test.get_by_tweet_id("1248231997048119305"))