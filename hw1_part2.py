# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  hw1_part2.py
# SDK(s):   Apache Beam

'''This workflow parses network log files by IP address and computes the total 
number of bytes served by each IP. It returns the top-K IPs that were served 
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
                        help='Path of output/results file.',
                        required=True)
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

    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    # Sum the content size (bytes) for each IP occurence.
    def sum_bytes(ip_cbyte):
      logging.info('sum_bte : [%s]', ip_cbyte)
      #(ip, cbyte) = ip_cbyte
      #byte_sum = sum(cbyte)
      return ip_cbyte
    
    def sort_bytes(ip_cbyte):
      logging.info('Before : [%s]', ip_cbyte)
      logging.info('After : [%s]', sorted(ip_cbyte))  
      return [ip_cbyte]

    # Define pipline for reading access logs, grouping IPs, summing the size,
    # and returning only top-K
    IpSizePcoll = (p | 'ReadAccessLog' >> (beam.io.ReadFromText(log_in)) \
                    | 'GetIpSize' >> beam.ParDo(ParseLogFn()) \
                    | 'Grouped' >> beam.GroupByKey() \
                    | 'SumSize' >> beam.Map(sum_bytes))

    SortPcoll = IpSizePcoll | 'Sort' >> beam.CombineGlobally(sort_bytes)
#                    | 'TopK' >> beam.ParDo(ReturnTopFn())#,top_k) 
#                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn())

    # Write to output file
    SortPcoll | beam.io.WriteToText(res_out)
    
    # Execute the Pipline
    result = p.run()
    result.wait_until_finish()

class ReturnTopFn(beam.DoFn):
  def process(self,grouped_ips):#,K):
    # group should be a tuple, sort it    
    tmp = sorted(grouped_ips,reverse=True)
    return tmp
    # if K == 0:
    #   logging.info('Sorted : [%s]', grouped_ips)
    #   return grouped_ips
    # else:
    #   logging.info('Sorted : [%s]', tmp[0:K])
    #   return tmp[0:K]



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
    #print(rawOutput[0])
    formattedOutput = "%s : %s " % (rawOutput[0],rawOutput[1])
    return [formattedOutput]

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()