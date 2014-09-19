#!/usr/bin/python
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

__author__ = 'api.nickm@gmail.com (Nick Mihailovski)'

import argparse
import sys

from apiclient.errors import HttpError
from apiclient import sample_tools
from oauth2client.client import AccessTokenRefreshError
from apiclient import discovery

import pandas
import json

from datetime import date, timedelta

def main(argv):
  # Authenticate and construct service.
  service, flags = sample_tools.init(
      argv, 'analytics', 'v3', __doc__, __file__,
      scope='https://www.googleapis.com/auth/analytics')
  
  # Try to make a request to the API. Print the results or handle errors.
  try:
    start_date = date(2014, 01, 01)
    day_count = 1
    for single_date in (start_date + timedelta(n) for n in range(day_count)):
        results = get_data(service, date)
        print_results(results)
    
    #print profiles
    #results = get_profile(service)
    #for e in results['items']:
    #    print e['id'] 
    
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


def get_data(service, date):
  """Executes and returns data from the Core Reporting API.

  This queries the API for Citroen Dealer web & mobile view.

  Args:
    service: The service object built by the Google API Python client library.

  Returns:
    The response returned from the Core Reporting API.
  """
  
  dimensions = dimensions = pandas.read_csv("custum_dim.csv", index_col=False, header=0)
  indices = list(dimensions[dimensions["selected"]==1]["index"])
  #dim = 'ga:deviceCategory' + ",".join(["ga:dimension" + str(i) for i in indices])
  dim = 'ga:networkLocation,ga:minute,ga:hour,ga:browser,ga:browserVersion,ga:screenResolution'
  
   
  return service.data().ga().get(
    ids='ga:80346422',
    start_date='2014-09-08',
    end_date='2014-09-19',
    metrics='ga:users',
    dimensions=dim,
    max_results='10000').execute()

def get_profile(service):
    return service.management().profiles().list(accountId='43986361', webPropertyId='UA-43986361-1').execute()

def print_results(results):
  """Prints out the results.

  This prints out the profile name, the column headers, and all the rows of
  data.

  Args:
    results: The response returned from the Core Reporting API.
  """

  print
  print 'Profile Name: %s' % results.get('profileInfo').get('profileName')
  print

  # Print header.
  output = []
  for header in results.get('columnHeaders'):
    output.append('%30s' % header.get('name'))
  print ''.join(output)

  # Print data table.
  nrow = 0
  if results.get('rows', []):
    for row in results.get('rows'):
      nrow += 1  
      output = []
      for cell in row:
        output.append('%30s' % cell)
      print ''.join(output)
    print str(nrow) + " found"
    
  else:
    print 'no rows found'


if __name__ == '__main__':
  main(sys.argv)
