#!/usr/bin/env python


################################################################################
# Copyright (c) 2010 Murty Gudipati
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
################################################################################

import mincemeat
import glob

text_files = glob.glob('hw3data/c*')

# returns the file contents
def file_contents(file_name):
  f = open(file_name)
  try:
    return f.read()
  finally:
    f.close()

# map function
def mapfn(key, val):
  for line in val.split('\r\n'):
    arr = line.split(':::')
    #['books/idea/becker2003/SerranoCP03', 'Manuel Serrano::Coral Calero::Mario Piattini', 'Metrics for Data Warehouse Quality.']  
    if len(arr) == 3:
      for word in arr[2].split():
        #['Metrics', 'for', 'Data', 'Warehouse', 'Quality.']
        for auth in arr[1].split('::'):
          #['Manuel Serrano', 'Coral Calero', 'Mario Piattini']
          yield auth+':'+word, 1 #['Manuel Serrano:Metrics' => 1]

# reduce function
def reducefn(key, val):
  result = sum(val)
  return result

# setup the server
# set the source as a dictionary of file name => file contents
server = mincemeat.Server()
server.datasource = dict((file_name, file_contents(file_name)) for file_name in text_files)
server.mapfn = mapfn
server.reducefn = reducefn

# start the server
results = server.run_server(password="changeme")
print results
