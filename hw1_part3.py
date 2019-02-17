# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  hw1_part3.py
# SDK(s):   Apache Beam

'''This file extends hw1_part2.py by adding sort/filter functionality. s the top-K IPs that were served 
the most number of bytes 

 References:
 1. https://beam.apache.org/documentation/programming-guide/
 2. https://beam.apache.org/documentation/sdks/python/
 3. https://beam.apache.org/get-started/wordcount-example/
 4. http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html 
'''
import argparse
import logging
import sys
import re
from datetime import datetime

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
import os



def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i',
                        dest='input',
                        help='Path of input file to process.',
                        required=True)
    parser.add_argument('--output', '-o',
                        dest='output',
                        default='output.txt',
                        help='Path of output/results file.')
    parser.add_argument('--K','-K',
                        dest='top_k',
                        type=int,
                        default=0,
                        help='Switch to return only top K IPs that were ' \
                              + 'served. Default: 0 (all)')

    args = parser.parse_args()
    log_in = args.input
    res_out = args.output
    top_k = args.top_k

    # Start Beam Pipeline
    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    # Define pipline for reading access logs, grouping IPs, summing the size,
    # and returning only top-K
    ip_size_pcoll = (p | 'ReadAccessLog' >> beam.io.ReadFromText(log_in)
                    | 'GetIpSize' >> beam.ParDo(ParseLogFn())
                    | 'Timestamp' >>  beam.ParDo(AddTimestampFn())
                    | 'Window' >> beam.WindowInto(window.FixedWindows(60*60))
                    | 'GroupIps' >> beam.CombinePerKey(sum)   
                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn()))

    # Write to output file
    ip_size_pcoll | beam.io.WriteToText(res_out)
    
    # Execute the Pipline
    result = p.run()
    result.wait_until_finish()

# Transform: parse log, returning string IP and integer size
# Expected example format:
# |  IP/Server  |  Date/Time |  M |   Location   |HTTP Ver.| M |Size |   
# |       0     |      1     |  2 |       3      |    4    | 5 |6(-1)|   
# -----------------------------------------------------------------------------
# 141.243.1.172 [29:23:53:25] "GET /Software.html HTTP/1.0" 200 1497
# -----------------------------------------------------------------------------
class ParseLogFn(beam.DoFn):
  def process(self,element):
    element_uni = element.encode('utf-8')
    try:
      elements = element_uni.split(" ")
    except ValueError:
      # keep track of how many times we fail to split/parse and
      # report status to log
      logging.info('Failed to split line : [%s]', element_uni)
      return [("Failed Parse",1)]
    
    ip = bytes(elements[0])
    # we have to make sure size is an integer, else set to 0
    try:
      size = int(elements[-1])
    except ValueError:
      # Handle the exception
      logging.debug('Size not an integer: [%s]', element_uni)
      size = 0

    # Part 3: extended to parse and convert time for unix time-stamp
    dtz = elements[1].translate(None, '[]')
    logging.debug('Date-time-zone : %s', dtz)
    try:
      unix_ts = datetime.strptime(dtz, "%d:%H:%M:%S").strftime('%s')
    except ValueError:
      unix_ts = str(0) # failed to get timestamp
      logging.info('Unable to parse date-time from \"%s\"', element_uni)
    
    logging.info('(ip,size,timestamp) : (%s,%s,%s)', ip,str(size),str(unix_ts))
    return [(ip,size,unix_ts)]

# Transform: adds timestamp to element
# Ref: Beam Programming Guide
class AddTimestampFn(beam.DoFn):
  def process(self, element):
    timestamp = element[2]
    new_element = (element[0],element[1])
    yield beam.window.TimestampedValue(new_element, int(timestamp))

# Transform: format the output as 'IP : size'
class FormatOutputFn(beam.DoFn):
  def process(self,rawOutput):
    # rawData is a list of strings/bytes
    formattedOutput = "%s : %s " % (rawOutput[0],rawOutput[1])
    return [formattedOutput]

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()