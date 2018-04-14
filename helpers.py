import pandas as pd
import numpy as np
import pprint as pp
import re, string
import time
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import praw
import credentials as creds

def getThreads(subreddit, limit = 20):
    thread_ids = []
    
    hotThreads = subreddit.hot(limit = limit)
    time.sleep(2)
    topThreads = subreddit.top(limit = limit)
    time.sleep(2)
    newThreads = subreddit.new(limit = limit)    
    
    for t in hotThreads:
        if (t.id not in thread_ids):
            thread_ids.append(t.id)
        
    for t in topThreads:
        if (t.id not in thread_ids):
            thread_ids.append(t.id)
        
    for t in newThreads:
        if (t.id not in thread_ids):
            thread_ids.append(t.id)
    
    return(thread_ids)


## ------------------------------------------------------------------------------------------

def getUsersFromSubreddit(subredditIds, r):
    if(len(subredditIds) < 1):
        return None
    
    usernames = []
    
    ## this process takes a while...    

    for thread_id in subredditIds:        
        thread = r.submission(id = thread_id)
        time.sleep(2)
        
        try:
            username = thread.author.name # gets the author of the thread 
            if (username is not None and username not in usernames):
                print("--- Adding user:", username)
                usernames.append(username)
        except:
            print("------ Unable to get thread author. Moving on...")      

        # get all authors of the comments in the thread
        allComments = thread.comments.list()
        for c in allComments:
            try:
                username = str(c.author)
                if (username is not None and username not in usernames):
                    print("--- Adding user:", username)
                    usernames.append(username)
            except:
                print("------ failed. moving on...")
                
        time.sleep(2)
                
    return(usernames)      

## ------------------------------------------------------------------------------------------

def cleanText(text):
    text = text.replace(',', ' ').replace(':', ' ')\
                .replace('...', ' ')\
                .replace('?', ' ')\
                .replace('!', ' ')\
                .replace(';', ' ')\
                .replace('\n', ' ').replace('\r', '') # replace newlines and page breaks     
                
    text = re.sub(r'([^\s\w]|_)+', '', text) # remove non-alphanumeric characters but leave the spaces
    text = re.sub(' +',' ', text) # remove double spaces        
    return(text)

## ------------------------------------------------------------------------------------------

def getRedditor(username, r):
    try:     
        username = username.strip()
        user = r.redditor(username)
    except Exception as e: 
        print(e)
        return None
    
    return(user)

## ------------------------------------------------------------------------------------------

def getUserComments(username, r, commentsLimit = 1000):   
    userComments = []
    

    counter = 0
    while (counter <= 3):
        user = getRedditor(username, r)
        if (user is not None):
            break
        else:
            counter = counter + 1               
            print("------------ Retry #", counter, "for user:", username)
            time.sleep(15)
        
    if (user is None):
        print("------ ERROR: unable to retrieve user info:", username)
        return None  
    
    time.sleep(2)
    
    try:
        newComments = user.comments.new(limit = commentsLimit)
    except Exception as e: 
        print(e)
        print("------ ERROR: unable to retrieve new comments. moving on...")
        newComments = None
    
    time.sleep(2)
    
    try:
        hotComments = user.comments.hot(limit = commentsLimit)
    except Exception as e: 
        print(e)
        print("------ ERROR: unable to retrieve hot comments. moving on...")
        hotComments = None
    
    time.sleep(2)
    
    try:
        controversialComments = user.comments.controversial(limit = commentsLimit)
    except Exception as e: 
        print(e)
        print("------ ERROR: unable to retrieve controversial comments. moving on...")
        controversialComments = None

    # new comments    
    try:
        if (newComments is not None):
            for c in newComments:
                comment = cleanText(c.body)        
                if (comment not in userComments):
                    userComments.append(comment)
    except:
        print("Failed...moving on...")
    
    # hot comments
    try:
        if (hotComments is not None):
            for c in hotComments:  
                comment = cleanText(c.body)        
                if (comment not in userComments):
                    userComments.append(comment)
    except:
        print("Failed...moving on...")

    # controversial comments
    try:
        if (controversialComments is not None):
            for c in controversialComments:
                comment = cleanText(c.body)     
                if (comment not in userComments):
                    userComments.append(comment)
    except:
        print("Failed...moving on...")
        
    print("------ Retrieved", len(userComments), "comments for:", username)
    return(userComments)

## ------------------------------------------------------------------------------------------

def subExists(subreddit, r):
    exists = True
    try:
        r.subreddits.search_by_name(subreddit, exact = True)
    except:
        exists = False
    return(exists)

## ------------------------------------------------------------------------------------------

def scrapeCommentsFromSubreddit(subreddit, r):
    
    UserArray = []
    CommentArray = []  
    
    exists = subExists(subreddit, r)
    
    if (exists == False):
        print("ERROR: subreddit '", subreddit, "' does not exist...")
        return None
    
    sub = r.subreddit(subreddit)
    
    threads = getThreads(sub)
    
    if (len(threads) < 1):
        print("ERROR: unable to retrieve threads for subreddit:", subreddit)
    else:
        print("Retrieved ", len(threads), "threads...")
    
    users = getUsersFromSubreddit(threads, r)
    
    if (len(users) < 1):
        print("ERROR: unable to retrieve users for subreddit:", subreddit)
    else:
        print("Retrieved ", len(users), "users...")
    
    for user in users:
        try:
            userComments = getUserComments(user, r)

            if (userComments is not None):
                userComments = ".".join(userComments)
                UserArray.append(user)
                CommentArray.append(userComments)
        except:
            print("ERROR: something failed badly. moving on...")
        
    df = pd.DataFrame(UserArray, columns = ["Username"])
    df["Comments"] = CommentArray
    
    print("Outputting dataframe with ", len(df), "records")
    
    return(df)

## ------------------------------------------------------------------------------------------



