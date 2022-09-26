from email import utils
import logging
import json
import time
import traceback
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options import pipeline_options
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io import WriteToText

from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners import DataflowRunner

import google.auth
from IPython.core.display import display, HTML
from apache_beam import Map,FlatMap
import apache_beam.transforms.window as window
from google.cloud import pubsub_v1
from google.cloud import language_v1

import apache_beam as beam
import os
import re
from google.cloud import language_v1
from textblob import TextBlob

class CleanTweet(beam.DoFn):
    
    def process(self, element):
        tweet = element['tweet']
        tweet = tweet.lower()
        tweet = re.sub("[^ a-z]"," ",tweet)
        tweet = tweet.strip()
        return [{'tweet' : tweet}]
    
class getSentiments(beam.DoFn):
    def process(self, element):
        os.system('pip install textblob')
        from textblob import TextBlob
        tweet = element['tweet']
        subjectivity = TextBlob(tweet).sentiment.subjectivity
        polarity = TextBlob(tweet).sentiment.polarity
        
        if polarity < 0:
            sentiment =  'Negative'
        elif polarity == 0:
            sentiment =  'Neutral'
        else:
            sentiment = 'Positive'
        
        res = {'tweet' : tweet , 'polarity' : polarity , 'subjectivity' : subjectivity, 'sentiment' : sentiment}

        return [res]


class classifyTweets(beam.DoFn):
    def process(self, element):
        language_client = language_v1.LanguageServiceClient()

        tweet = element['tweet']

        document = document = language_v1.Document(content=tweet, type_=language_v1.Document.Type.PLAIN_TEXT)

        
        if(len(tweet.split()) >= 20):
            try:
                response = language_client.classify_text({"document" : document}).categories[0].name
            except:
                response = None
            pass
        else:
            response = None
        
        element["classification"] = response
        
        return element

def streaming_pipeline(project='real-time-streaming-362212', region="us-central1"):
    
    topic = "projects/{}/topics/twitter-streaming".format(project)
    bucket = "gs://real-time-streaming"
    table = "{}:dataset.twitter".format(project)
    schema  = "tweet:string,polarity:float64,subjectivity:float64,sentiment:string"
    options = PipelineOptions(
        streaming=True,
        project=project,
        region=region,
        staging_location="%s/staging" % bucket,
        temp_location="%s/temp" % bucket,
    )
    
    p = beam.Pipeline(DataflowRunner(), options=options)

    pubsub = (p | "Read Topic" >> ReadFromPubSub(topic = topic) | beam.Map(json.loads) | beam.ParDo(CleanTweet()) | beam.ParDo(getSentiments()))
    
    pubsub | "Write To BigQuery" >> WriteToBigQuery(table=table, schema=schema,
                              create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                              write_disposition=BigQueryDisposition.WRITE_APPEND)
    result = p.run()
    return result