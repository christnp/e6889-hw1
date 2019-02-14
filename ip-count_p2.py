#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  ip-count.py
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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os



def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        #default='',
                        help='Path of input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        #default='',
                        help='Path of output/results file.',
                        required=True)

    args = parser.parse_args()
    #log_in = args.input
    res_out = args.output
    log_in = "NASA_access_log_Aug95_test"

    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    # Sum the content size (bytes) for each IP occurence.
    def sum_bytes(ip_cbyte):
      (ip, cbyte) = ip_cbyte
      byte_sum = sum(cbyte)
      return [(ip, byte_sum)]

    # Create PCollection
    IpSizePcoll = p | 'ReadAccessLog' >> (beam.io.ReadFromText(log_in)) \
                    | 'GetIpSize' >> beam.FlatMap(lambda x: [(bytes(x.split(" ")[0]), \
                                                            int(x.split(" ")[-1]))]) \
                    | 'Grouped' >> beam.GroupByKey() \
                    | 'SumSize' >> beam.Map(sum_bytes)

    IpSizePcoll | beam.io.WriteToText(res_out)
    
    result = p.run()
    result.wait_until_finish()

class GetIpFn(beam.DoFn):
  def process(self, element):
    # split strig by whitespaces
    elements = element.split(" ")
    ip_size = [elements[0], elements[-1]]
    print(ip_size)
    return ip_size #20190213: added [] so it returns properly!

class GetSizeFn(beam.DoFn):
  def process(self, element):
    elements = element.split(" ")
    size = elements[-1]
    return [size]

if __name__ == '__main__':
  run()