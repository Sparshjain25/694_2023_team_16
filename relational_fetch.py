import pymysql
from ordered_hash_set import OrderedSet

class Relational():
    def __init__(self) -> None:
        pass

    # Method to establish connection the database
    def connect(self):
        # establish connection to database
        conn = pymysql.connect(user='***', \
            password='***', \
            host='***', \
            database='***')  
        cursor = conn.cursor()

        return cursor

    # Method to close the connection the database
    def disconnect(self,cursor):
        cursor.close()

    # Get all the possible users whenever an user is searched and order them by verified status and number of followers
    def get_users(self,str_input):
        cursor = self.connect()
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

        self.disconnect(cursor)

        return list(s)

    # Get the all the details of a particular user
    def get_user_details(self,str_input):
        cursor = self.connect()

        #Query which searches the username exactly
        cursor.execute("SELECT * FROM user WHERE id = '%s' \
        ORDER BY verified DESC, followers_count DESC"%str_input)

        res = cursor.fetchall()
        self.disconnect(cursor)
        return res
    
    # Get the top 20 handles, i.e, users having the most number of followers
    def top_handles(self):
        cursor = self.connect()

        cursor.execute("SELECT * FROM user ORDER BY followers_count DESC LIMIT 20")

        res = cursor.fetchall()
        self.disconnect(cursor)
        return list(res)
    
    # Get the top 20 celebrities, i.e., users who are verified and having the most number of followers
    def top_celebs(self):
        cursor = self.connect()

        cursor.execute("SELECT * FROM user ORDER BY verified DESC, followers_count DESC LIMIT 20")

        res = cursor.fetchall()
        self.disconnect(cursor)
        return list(res)

    # Get the top 20 active users with the most number tweets
    def top_actives(self):
        cursor = self.connect()

        cursor.execute("SELECT * FROM user ORDER BY statuses_count DESC LIMIT 20")

        res = cursor.fetchall()
        self.disconnect(cursor)
        return list(res)
