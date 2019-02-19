# -*- coding: utf-8 -*-
#!/usr/bin/env python2

# Columbia University, 2019
# ELEN-E6889: Homework 1
# 
# Author:   Nick Christman, nc2677
# Date:     2019/02/19
# Program:  hw1_part2.py
# SDK(s):   Apache Beam

'''This file extends hw1_part1.py by adding sort/filter functionality. The top-K
IPs that were served are saved to an output file. The default output is a sorted
list of IP:Size tuples. 

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
                    | 'GroupIps' >> beam.CombinePerKey(sum)) 

    # Part 2: extended Pipeline from Part 1; ip_size_pcoll PCollection is 
    #         combined as a single list of tuples (ip,size) and then sorted 
    #         serially. The final list is then formatted serially
    # TODO: figure out a more effecient method.
    def sort_bytes(ip_cbyte,k=0):
      # Ref: https://stackoverflow.com/questions/3121979/how-to-sort-list-tuple-of-lists-tuples
      if k == 0:
        top_k = ip_cbyte # not sorted
      else:
        top_k = sorted(ip_cbyte, key=lambda tup: tup[1], reverse=True)[:k]  
      return top_k

    top_k_pcoll = (ip_size_pcoll | 'CombineAsList' >> beam.CombineGlobally(
                                              beam.combiners.ToListCombineFn())
                    | 'SortTopK' >> beam.Map(sort_bytes,top_k)
                    | 'FormatOutput' >> beam.ParDo(FormatOutputFn()))

    # Write to output file
    top_k_pcoll | beam.io.WriteToText(res_out)
    
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

    return [(ip,size)]

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