#!~/anaconda/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple intro to using the Google Analytics API v3.

This application demonstrates how to use the python client library to access
Google Analytics data. The sample traverses the Management API to obtain the
authorized user's first profile ID. Then the sample uses this ID to
contstruct a Core Reporting API query to return the top 25 organic search
terms.

Before you begin, you must sigup for a new project in the Google APIs console:
https://code.google.com/apis/console

Then register the project to use OAuth2.0 for installed applications.

Finally you will need to add the client id, client secret, and redirect URL
into the client_secrets.json file that is in the same directory as this sample.

Sample Usage:

  $ python hello_analytics_api_v3.py

Also you can also get help on all the command-line flags the program
understands by running:

  $ python hello_analytics_api_v3.py --help
"""

import argparse
import sys

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
from apiclient import discovery

import pandas
import json
import csv 
import hashing

from datetime import date, timedelta

def main(argv):
    
  # Parse parameters.
  parser = argparse.ArgumentParser(description='Extract ga data over a time range.')
  parser.add_argument('start', help="beginning of the dates range (yyyymmdd), ex: 20131201")
  parser.add_argument('stop', help="end of the dates range (yyyymmdd), ex: 20140202")
  parser.add_argument('view', help="ga view for the extract, ex: ga:80346422", default="ga:80346422")
  parser.add_argument('dir', help="location for the csv outputs (one per day)", default="files")
  parser.add_argument('cust_dim', help="csv file specifying the custum dimensions to extract", default="custum_dim.csv")
  parser.add_argument('nat_dim', help="csv file specifying the native dimensions to extract", default="native_dim.csv")
  parser.add_argument('key', help="csv file specifying the native dimensions to extract", default="ga:networkLocation,ga:minute,ga:hour,ga:browser,ga:browserVersion,ga:screenResolution")
  parser.add_argument('metric', help="the metric to extract", default="ga:users")
  args = parser.parse_args()
  
  # Authenticate and construct service.
  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics')
  
  custum_dims = pandas.read_csv(args.cust_dim, index_col=False, header=0)
  native_dims = pandas.read_csv(args.nat_dim, index_col=False, header=0)
  dimensions = list(custum_dim[dimensions["selected"]==1]["dimension"]) + list(native_dims[dimensions["selected"]==1]["dimension"])  
  start_date = date(int(args.start[0:4]), int(args.start[4:6]), int(args.start[6:8]))
  stop_date = date(int(args.stop[0:4]), int(args.stop[4:6]), int(args.stop[6:8]))
  day_count = (stop_date - start_date).days
    
  # Try to make a request to the API. Print the results or handle errors.
  try:
    for dt in (start_date + timedelta(n) for n in range(day_count)):
        for dim in dimensions:
            results = get_data(service, str(dt), args.view, args.key, dim, args.metric)
            get_results(results, dim)
    
  except TypeError, error:
    # Handle errors in constructing a query.
    print ('There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
    print ('Arg, there was an API error : %s : %s' %
           (error.resp.status, error._get_reason()))

  except AccessTokenRefreshError:
    # Handle Auth errors.
    print ('The credentials have been revoked or expired, please re-run the application to re-authorize')


def get_data(service, view, date, key, dim):
  """Executes and returns data from the Core Reporting API.

  Args:
    service: The service object built by the Google API Python client library
    view: String, the view from which to extract in google analytics, ex: 'ga:80346422'
    date: String, day of the extraction, ex: 'yyyy-mm-dd'
    key: String, a comma separated list of the google analytics fields which compose the key, ex: 'ga:hour,ga:minute'
    dim: String, the dimension to extract, ex: 'ga:dimension12'
    metric: String, the metric to extract, ex: 'ga:users'
    
  Returns:
    The response returned from the Core Reporting API.
  """
  
  return service.data().ga().get(
    ids=view,
    start_date=date,
    end_date=date,
    metrics=metric,
    dimensions=key + "," + dim,
    max_results='10000000').execute()

def get_results(results, dimension, label, metric):
  """Get the results from get_data().

  Args:
    results: The response returned from the Core Reporting API
    dimension: String, the dimension to extract, ex: 'ga:dimension12'
    label: String, label of the dimension to extract, ex: 'vehicleSeatsNumber'
    metric: String, the metric extracted, ex: 'ga:users'
  """
  
  # get the result's headers.
  headers = []
  for header in results.get('columnHeaders'):
    headers.append(header.get('name'))
  
  dim_location = headers.index(dimension) # look for the dimension position 
  metric_location = headers.index(metric) # look for the metric position
  m = hashlib.md5() # initialize the hash function
  df = pandas.DataFrame(columns=['key', label]) # make an empty data frame
  nrow = 0
  
  if results.get('rows', []):
    for row in results.get('rows'):
      nrow += 1  
      output = []
      for cell in row:
        output.append(cell)
      output.pop(metric_location) # remove the metric
      m.update("".join(output)) # hash the concatenation of the fields which compose the key
      val = output.pop(dim_location) # get the dimension value
      key = m.hexdigest()
      df.loc[len(df)+1] = [key, val] # add the new row to the data frame
      print ''.join(output)
      print [key, val]

  return df
  
if __name__ == '__main__':
  main(sys.argv)
