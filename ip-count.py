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

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        #default='',
                        help='Path of input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        #default='',
                        help='Path of output/results file.')

    args = parser.parse_args()
    #log_in = args.input
    #res_out = args.output
    log_in = "NASA_access_log_Aug95"
    p = beam.Pipeline(options=PipelineOptions())
    # No runner specified -> DirectRunner used (local runner)

    lines = p | 'ReadAccessLog' >> beam.io.ReadFromText(log_in)


if __name__ == '__main__':
  run()