import re
import time
import json
from google.cloud import pubsub_v1


PROJECT = "events-pipeline-challenge"
TOPIC = "projects/events-pipeline-challenge/topics/tracking_events"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, TOPIC)


def clean_json(events_raw):
    """
    This function helps with the cleanup of the events provided.
    Normally each event would be passed independently, however
    this function takes the full json events load ad splits it so
    the processing can be done one element at a time.
    :param events_raw: String
    :return: clean_events_list: list of Strings messages ready to be loaded as JSON
    """
    clean_events_list = []
    regex_replace = [(r"([ \{,:\[])(u)?'([^']+)'", r'\1"\3"'), (r" False([, \}\]])", r' false\1'),
                     (r" True([, \}\]])", r' true\1')]
    for r, s in regex_replace:
        event_raw_json = re.sub(r, s, events_raw)
    clean_events = events_raw.replace('\xe2\x80\x9c','"')\
        .replace('\xe2\x80\x9d','"')\
        .replace('},', '}&,&')  #Remove bad quotes and create special split expression for JSON
    for element in clean_events.split('&,&'):
        clean_events_list.append(element)
    return clean_events_list

def publish(publisher, topic_path, event):
    data = event.encode('utf-8')
    return publisher.publish(topic_path, data=data)


def callback(event_result):
    if event_result.exception(timeout=15):
        print('Exception on {0}: {1}.'.format(
            topic_path, event_result.exception()))
    else:
        print(event_result.result())


if __name__ == '__main__':
    with open('events.json', 'r') as f:
        events_raw = f.read()
    clean_events_list = clean_json(events_raw)
    for event in clean_events_list: #This loop would not exist on a normal streaming platform
        event_result = publish(publisher, topic_path, event)
        event_result.add_done_callback(callback)
    time.sleep(1.1)