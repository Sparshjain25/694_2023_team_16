from flask import Flask, request,render_template
from fetch_code import *
from relational_fetch import *
from cache import *

cache=LRUCache()
app = Flask(__name__)

@app.route("/home",methods=["GET"])
def home():    
    if request.method == "GET":
        return render_template("homepage.html")

@app.route("/home/searched_results",methods=["POST"])
def searched():
    if request.method=="POST":
        input=request.form["text"]
        if input[0]=='@':
            return GET_USERS(input[1:])
        elif input[0]=='#':
            return GET_HASH(input[1:])
        else:
            return GET_TWEETS(input)
            
@app.route("/home/searched_results/tweets_timed",methods=["POST"])
def timed_tweets():
    if request.method=="POST":
        time_range=request.form["my_dropdown"]
        input=request.form["custom_value"]
        test=Non_Relational()
        strt=time.time()
        lists=test.get_tweet_by_time(input,time_range)
        end=time.time()
        exec=f"{end-strt:.10f}"
        return render_template('tweetpage_(1).html', data = lists, time=exec, range=len(lists),flag="Database",title = "Timed Tweets",)
    
@app.route("/home/searched_results/hashes_timed",methods=["POST"])
def timed_hashtags():
    if request.method=="POST":
        time_range=request.form["my_dropdown"]
        input=request.form["custom_value"]
        test=Non_Relational()
        strt=time.time()
        lists=test.get_hashtags_by_time(input,time_range)
        end=time.time()
        exec=f"{end-strt:.10f}"
        return render_template('hashtagpage_(1).html', data = lists, time=exec, range=len(lists),flag="Database",title = "Timed Hashtags",)



@app.route("/home/searched_results/tweet/<id>")
def Quoted(id):
    test=Non_Relational()
    strt=time.time()
    lists=test.get_by_tweet_id(id)
    end=time.time()
    exec=f"{end-strt:.10f}"
    return render_template('tweetpage_(1).html', data = lists[:min(len(lists),50)], time=exec, flag="Database",title = "Main ID",)

@app.route("/home/searched_results/users/<id>")
def user(id):
    test=Relational()
    str=time.time()
    lists=test.get_user_details(id)
    end=time.time()
    exec=f"{end-str:.10f}"
    return render_template('userpage_(1).html', data = lists, time=exec, flag="Database",title = "Searched User",)


@app.route("/home/user_tweets/<id>")
def get_user_tweets(id):
    test=Non_Relational()
    str=time.time()
    lists=test.get_by_user(id)
    end=time.time()
    exec=f"{end-str:.10f}"
    return render_template('tweetpage_(1).html', data = lists,range=len(lists),time=exec, flag="Database",title = "User",)

@app.route("/home/searched_results/retweets_users/<id>")
def get_retweets(id):
    test=Non_Relational()
    strt=time.time()
    lists=test.get_retweets(id)
    end=time.time()
    exec=f"{end-strt:.10f}"
    return render_template('retweetpage_(1).html',data=lists,time=exec,flag="Database",title="Retweet")

@app.route("/home/Top_20_handles")
def top_users():
    test=Relational()
    strt=time.time()
    l=test.top_handles()
    end=time.time()
    exec=f"{end-strt:10f}"
    return render_template('userpage_(1).html', data = l, time=exec, flag="Database",title = "Top Handles",)

@app.route("/home/Top_20_Celebs")
def top_celebs():
    test=Relational()
    strt=time.time()
    l=test.top_celebs()
    end=time.time()
    exec=f"{end-strt:10f}"
    return render_template('userpage_(1).html', data = l, time=exec, flag="Database",title = "Top Celebs",)

@app.route("/home/Top_20_Active_Handles")
def top_active():
    test=Relational()
    strt=time.time()
    l=test.top_actives()
    end=time.time()
    exec=f"{end-strt:10f}"
    return render_template('userpage_(1).html', data = l, time=exec, flag="Database",title = "Top Active",)

@app.route("/home/Top_20_tweets")
def top_tweets():
    test=Non_Relational()
    strt=time.time()
    l=test.top_tweets()
    end=time.time()
    exec=f"{end-strt:10f}"
    return render_template('toptweetspage.html', data = l, range=20,time=exec, flag="Database",title = "Top Tweets",)

def GET_USERS(input):
    test=Relational()
    if(cache.get("@"+input) !=None):
        str=time.time()
        res=cache.get("@"+input)
        end=time.time()
        exec=f"{end-str:.10f}"
        return render_template('userpage_(1).html', data = res, time=exec, flag="Cache",title = input,)
    else:
        str=time.time()
        lists=test.get_users(input)
        end=time.time()
        exec=f"{end-str:.10f}"
        cache.put("@"+input,lists)
        return render_template('userpage_(1).html', data = lists,time=exec,flag="Database", title = input,)

def GET_TWEETS(input):
    test=Non_Relational()
    if(cache.get(input) !=None):
        str=time.time()
        res=cache.get(input)
        end=time.time()
        exec=f"{end-str:.10f}"
        return render_template('tweetpage_(1).html', data = res, time=exec, range=len(res),flag="Cache",title = input,)
    else:
        str=time.time()
        lists=test.get_tweets(input)
        end=time.time()
        cache.put(input,lists)
        exec=f"{end-str:.10f}"
        return render_template('tweetpage_(1).html', data = lists,time=exec,range=len(lists),flag="Database", title = input,)

def GET_HASH(input):
    test=Non_Relational()
    if(cache.get("#"+input) !=None):
        str=time.time()
        res=cache.get("#"+input)
        end=time.time()
        exec=f"{end-str:.10f}"
        return render_template('hashtagpage_(1).html', data = res, time=exec, range=len(res),flag="Cache",title = input,)
    else:
        str=time.time()
        lists=test.get_hashtags(input)
        end=time.time()
        cache.put("#"+input,lists)
        exec=f"{end-str:.10f}"
        return render_template('hashtagpage_(1).html', data = lists,time=exec,range=len(lists),flag="Database", title = input,)
    

