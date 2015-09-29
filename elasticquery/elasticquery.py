#  Copyright 2014 Science & Technology Facilities Council
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
import sys 
import urllib
from json import load
from urllib2 import urlopen, HTTPError
from prettytable import PrettyTable
from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from argparse import ArgumentParser

with open('/opt/elasticquery/elasticquery.json') as config_file:
    config = load(config_file)

parser = ArgumentParser(description='Search CASTOR logs.')
parser.add_argument('field', type=str, help='')
parser.add_argument('value',type=str, help='')
parser.add_argument('--days', type=int, default=1, help='')
parser.add_argument('--hours', type=int,default=1, help='')
 
args = parser.parse_args() 
now = datetime.now()

def date_info():
  return { 
    'year' : now.year,
    'month' : now.month,
    'day' : now.day,
    'hour' : now.hour,
  }
fields = config['field'] 
table= PrettyTable(field_names=fields)
le = []
for day in range (args.days):

 for hour in range(args.hours):
   
    url = 'http://%(server)s:%(port)s/%(index)s/_search?size=100&q='%config
    url = url % date_info()
    url += '%(field)s:%%22%(value)s%%22' % vars(args)
    query = urllib.quote(url, safe="%/:=&?~#+!$,;'@()*")
   
    print '%(year)d-%(month)02d-%(day)02d' % date_info()
      
    try:
        results = load(urlopen(query))       
        if results['hits']['total'] > 0:
            
           print "  %d results found" % (results['hits']['total'])
           for message in results['hits']['hits']:
                message = message['_source']
                line = []
                for field in fields:
                    if field in message:
                        line.append(message[field])
                    else:
                        line.append("")
                le = line
                table.add_row(le)

        else:
          
            print "  No results found on the day"

    except HTTPError:

        print "  No index for this day"

    now -= timedelta(days=1,hours=1)
    print 
    

print table.get_string(sortby='@timestamp')
