#!/usr/bin/env python
# coding: utf8

import rawes
from rawes.elastic_exception import ElasticException
import requests
from requests.exceptions import Timeout, ConnectionError
import sys, traceback, time
import json

bulk_data = ''
bulk_size = 100

total = 0
number = 0

url = 'YOUR URL'
endpoint =  'dz-music/users/_bulk'

es = rawes.Elastic(url, auth=('YOUR USERNAME', 'YOUR PASSWORD'))

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