#!/usr/bin/env python
# coding: utf8

import rawes
from rawes.elastic_exception import ElasticException
import requests
from requests.exceptions import Timeout, ConnectionError
import fileinput
import sys, traceback, time
import smtplib
from email.mime.text import MIMEText
import socket
import json

bulk_data = ''
bulk_size = 100

total = 0
number = 0

url = 'http://832231001b9bffdc56aac9e6805a39f7.eu-west-1.aws.found.io:9200'
endpoint =  'dz-music/users/_bulk'

es = rawes.Elastic(url, auth=('aurelien', 'pfnighado45klnbgls'))
#~ conn = ES('localhost:9200', timeout, bulk_size, None, None, max_retries)

#~ head = """{"index":{"_type":\""""+ _type +"""\","_index":\""""+ _index +"""\"}}"""
head = json.dumps({ "index" : { } })

for line in sys.stdin:
    line = line.rstrip('\r\n')


    bulk_data += head + '\n' + line +  '\n'
    total += 1
    number += 1

    if number >= bulk_size:
        try:
            result = es.post(endpoint, data=bulk_data)
            print time.strftime('%Y-%m-%d %H:%M:%S') + " Indexed bulk : #"+str(bulk_size)+", total : "+str(total)
        except (ElasticException, Timeout, ConnectionError) as e:
            print e

        bulk_data = ''
        number = 0

if number > 0:
    try:
        result = es.post(endpoint, data=bulk_data)
        print time.strftime('%Y-%m-%d %H:%M:%S') + " Total indexed : #"+str(total)
    except (ElasticException, Timeout, ConnectionError) as e:
        print 'error'