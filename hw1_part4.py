# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  hw1_part4.py
# SDK(s):   Apache Beam

'''This file extends hw1_part3.py by adding windowing functionality.

 References:
 1. https://beam.apache.org/documentation/programming-guide/
 2. https://beam.apache.org/documentation/sdks/python/
 3. https://beam.apache.org/get-started/wordcount-example/
 4. http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html 
'''
import argparse
import logging
import sys
import os
import re
import ipaddress
import socket
from datetime import datetime
import dateutil.parser as parser


import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
import os



def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', '-i',
                        dest='input',
                        help='Path of input file to process.If only name is ' \
                          'provided then local directory selected.',
                        required=True)
    parser.add_argument('--output', '-o',
                        dest='output',
                        default='output.txt',
                        help='Path of output/results file. If only name is ' \
                          'provided then local directory selected.')
    parser.add_argument('--K','-K',
                        dest='top_k',
                        type=int,
                        default=0,
                        help='Switch to return only top K IPs that were ' \
                              + 'served, sorted from most to least served (in ' \
                              + 'bytes) Default: 0 (return all, not sorted)')
    parser.add_argument('--prefix','-p',
                        dest='prefix',
                        default="255.255.255.255",
                        help='Specify subnet mask (prefix). Default: 255.255.' \
                          '255.255 (no prefix). Any numeric value not-equal ' \
                            'to \"255\" implies mask/prefix will be applied.')

    args = parser.parse_args()
    log_in = args.input
    res_out = args.output
    top_k = args.top_k
    prefix = args.prefix

    # function from Part 2 to sort by top-K served IPs (by prefix now)
    def sort_bytes(ip_cbyte,k=0):
      # Ref: https://stackoverflow.com/questions/3121979/how-to-sort-list-tuple-of-lists-tuples
      if k == 0:
        top_k = ip_cbyte # not sorted
      else:
        top_k = sorted(ip_cbyte, key=lambda tup: tup[1], reverse=True)[:k]  
      return top_k

    # Start Beam Pipeline
    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    # Define pipline for reading access logs, grouping IPs, summing the size,
    # and returning only top-K
    ip_size_pcoll = (p | 'ReadAccessLog' >> beam.io.ReadFromText(log_in)
                    | 'GetIpSize' >> beam.ParDo(ParseLogFn())
                    | 'PrefixMask' >>  beam.ParDo(ApplyPrefixMaskFn(),prefix)
                    | 'GroupIps' >> beam.CombinePerKey(sum)
                    | 'CombineAsList' >> beam.CombineGlobally(
                                              beam.combiners.ToListCombineFn())
                    | 'SortTopK' >> beam.Map(sort_bytes,top_k)
                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn()))

    # Write to output file as text     
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
  # helper function to create datetime object from string
  def __dt_check(self,dt_raw):    
    # first, try to auto parse the date/time. If it fails, force a date/time
    try:
      dt_obj = parser.parse(dt_raw)
      logging.debug('__dt_check() date/time : %s', dt_obj)      
    except:
      logging.debug('__dt_check() failed to parse date/time \"%s\"', dt_raw)
      # Force date to be start of Unix Epoch (01/01/1970). Naively assume the 
      # date time to be DD:HH:MM:SS and naively assume that the time is 
      # HH:MM:SS at the end
      dt_split = dt_raw.split(":")
      dt_time =  dt_split[-3:]
      # for some reason there is no day...
      try:
        dt_day = dt_split[-4]
      except:
        dt_day = 1

      dt_obj = datetime(1970, 1, int(dt_day),int(dt_time[0]),
                      int(dt_time[1]),int(dt_time[2]))    
    return dt_obj
  # Main function
  def process(self,element):
    element_uni = element.encode('utf-8')
    try:
      elements = element_uni.split(" ")
    except ValueError:
      # keep track of how many times we fail to split/parse and
      # report status to log
      logging.info('Failed to split line : [%s]', element_uni)
      return [("Failed Parse",1)]
    
    # parse IP address
    ip = bytes(elements[0])
    
    # parse size; set to 0 if non-integer
    try:
      size = int(elements[-1])
    except ValueError:
      # Handle non-integer exception
      logging.debug('Size not an integer: [%s]', element_uni)
      size = 0

    # Part 3: extended to parse and convert time for unix time-stamp
    # parse date/time; strip out brackets (assumes [date/time] format)
    try:
      dt_str_raw = elements[1].translate(None, '[]')
      dt = self.__dt_check(dt_str_raw)
    except:
      dt = datetime.utcfromtimestamp(0)
    # compute the timestamp
    unix_ts = (dt - datetime.utcfromtimestamp(0)).total_seconds()

    logging.debug('Date-time: %s', dt)    
    logging.debug('Timestamp: %s', unix_ts)

    return [(ip,size,unix_ts)]

# Transform: applies mask/prefix and adds timestamp to element
# Ref: Beam Programming Guide
class ApplyPrefixMaskFn(beam.DoFn):
  def process(self,element,prefix):
    logging.debug('ApplyPrefixMaskFn() prefix: %s', prefix)
    logging.debug('ApplyPrefixMaskFn() IP Addr: %s', str(element[0]))

    # split inputs for comparison
    prefix_split = prefix.split(".")
    element_split = element[0].split(".")
    
    # use socket module to check if it is an IP address vs. weblink
    try: 
      socket.inet_aton(element[0]) #true if IPv4, error if weblink

      # this allows for non-standard masking (e.g., A.*.C.D, A.B.*.D, etc.)
      if int(prefix_split[3]) != 255:
        element_split[3] = "*"
      if int(prefix_split[2]) != 255:
        element_split[2] = "*"
      if int(prefix_split[1]) != 255:
        element_split[1] = "*"
      if int(prefix_split[0]) != 255:
        element_split[0] = "*"
      
    # must be a weblink? Aggregate by first two words
    except:
      element_split[2:] = "*" # example: a.b.x.y.z => a.b.*

    # join the sub-elements to form the filtered IP address
    tmp_element = '.'.join(element_split)  
    logging.debug('ApplyPrefixMaskFn() IP Addr Web: %s', tmp_element)

    yield beam.window.TimestampedValue((tmp_element,element[1]),int(element[2]))
    

# Transform: format the output as 'IP : size'
class FormatOutputFn(beam.DoFn):
  def process(self,rawOutputs):
    # define output format
    formatApply = "{:7d} byte(s) were served to {:s}"
    # loop through (presumably) sorted list
    formattedOutput = []
    for rawOutput in rawOutputs:
      formattedOutput.append(formatApply.format(rawOutput[1],rawOutput[0]))
    
    logging.debug('FormatOutputFn() %s', formattedOutput)
    return formattedOutput

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()