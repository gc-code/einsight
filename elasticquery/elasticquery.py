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

# NOTE: Many of elasticsearch commands have been commented with useful information
#       on their usage. For a very detailed view of how elasticsearch works please
#       consult the online documentation.

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

# Parse command line arguments
parser = ArgumentParser(description='Search CASTOR logs.')
parser.add_argument('field', type=str, help='')
parser.add_argument('value',type=str, help='')
parser.add_argument('--days', type=int, default=1, help='')
parser.add_argument('--hours', type=int,default=1, help='')
parser.add_argument('-n', type=int, dest="num_results", default=100, help='')
 
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

num_results = args.num_results
le = []
for day in range (args.days):

 for hour in range(args.hours):
    # NOTE: for larger applications it will be worth using the Python API rather than the REST interface used here
    #
    # Here we specify the URL of the host using the command
    # http://hostname:port/index/_search?scroll=time&size=num_results&q=
    # index = Like a database in SQL. Likely to be 'logstash-<year>.<month>.<day>' in this case.
    # scroll = instructs the server to send all results within 'size' until the specified time has elapsed.
    #          If this option is not specified then elasticsearch reads a maximum of a few hundred entries.
    # size = The number of results to return from the query
    # q = The query to run (see below)
    url = 'http://%(server)s:%(port)s/%(index)s/_search?scroll=1h&size=' + str(num_results) + '&q='
    url = url % config  # Format the URL with information from the JSON configuration file
    url = url % date_info()

    # Create a query
    # q=field: "value"
    # Search the index for 'value' in 'field'.
    # A field is like a column in a relational database
    url += '%(field)s:%%22%(value)s%%22' % vars(args)
    query = urllib.quote(url, safe="%/:=&?~#+!$,;'@()*")    # Remove all characters in 'safe' from the URL

    # The query reply comes in the form of a JSON document with key : value pairs
   
    print '%(year)d-%(month)02d-%(day)02d' % date_info()
      
    try:
        results = load(urlopen(query)) # Connect and issue query

        # The query returns a dictionary from which results can be retrieved.
        # The dictionary key is the JSON field.
        if results['hits']['total'] > 0:
            
           print "  %d results found" % (results['hits']['total'])
           # Iterate over the results
           for message in results['hits']['hits']:
                # '_source' is the JSON document returned by the server
                # everything else is just meta-data.
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
