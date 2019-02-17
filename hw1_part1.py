# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  hw1_part1.py
# SDK(s):   Apache Beam

'''This workflow parses network log files by IP address and computes the total 
number of bytes served by each IP. 

 References:
 1. https://beam.apache.org/documentation/programming-guide/
 2. https://beam.apache.org/documentation/sdks/python/
 3. https://beam.apache.org/get-started/wordcount-example/
 4. http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html 
'''
import argparse
import logging

import apache_beam as beam
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

    args = parser.parse_args()
    log_in = args.input
    res_out = args.output

    # Start Beam Pipeline
    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    # Define pipline for reading access logs, grouping IPs, & summing the size.
    ip_size_pcoll = (p | 'ReadAccessLog' >> beam.io.ReadFromText(log_in)
                    | 'GetIpSize' >> beam.ParDo(ParseLogFn()) 
                    | 'GroupIps' >> beam.CombinePerKey(sum) 
                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn()))
# TODO: Aggregate address as query2.lycos.*.*

    # Write to output file
    ip_size_pcoll | beam.io.WriteToText(res_out)
    
    # Execute the Pipline
    result = p.run()
    result.wait_until_finish()

# Transform: parse log, returning string IP and integer size
# - Expected example format:
#   |    IP/Server   | X |    Date/Time        | TZ?  | M  | Location | M |Size    
#   |       0        |1|2|        3            |  4   | 5  |    6     | 7 |8(-1)   
# - duckling.omsi.edu - - [04/Jul/1995:18:38:41 -0400] "GET /HTTP/1.0" 200 7074
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

    return [(ip,size)]

# Transform: format the output as 'IP : size'
class FormatOutputFn(beam.DoFn):
  def process(self,rawOutput):
    # rawData is a list of strings/bytes
    formattedOutput = "%s : %s " % (rawOutput[0],rawOutput[1])
    return [formattedOutput]

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()