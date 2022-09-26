import os
from google.cloud import pubsub_v1
import tweepy
import logging
import time
import json

credentials_path = "real-time-streaming.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
publisher = pubsub_v1.PublisherClient()

topic = "projects/real-time-streaming-362212/topics/twitter-streaming"

Bearer_TOKEN = "#Twitter API Bearer Token"

query = '(meal OR juice OR soft drinks OR recipes OR cooking OR desserts OR soups OR stews OR Pizza OR pasta OR candy OR sweets OR food) KFC lang:en -is:retweet -has:links -is:reply'

while True:
    client = tweepy.Client(bearer_token=Bearer_TOKEN)
    tweets = client.search_recent_tweets(query=query,max_results=100)
    for msg in tweets.data:
        data = msg.text
        data = json.dumps({'tweet' : data}).encode('utf-8')
        future = publisher.publish(topic,data)
        print(f"Published message : {future.result()}")
    time.sleep(200)