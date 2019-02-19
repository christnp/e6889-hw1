# ELEN-E6889 Homework 1
#### By:  Nick Christman
#### Due: 02.19.2018

## Assumptions:
1. Log file is saved in proper, UTF-8 encoding
2. Log file format is consistent (see source code comment for ParseLogFn())

## Discussion:
### Part 1 - 
### Part 2 - 
### Part 3 -
### Part 4 - 

## Notes:
I did not mask web links, except when specifially told to (i.e., Part 4)

Make sure that you follow the instructions of deliverability. Please put the codes for each part into a separate folder and name them "hw1_part1", "hw1_part2", "hw1_part3" and "hw1_4".  Your final submission should include four aforementioned folders, a report and the data file you use.

Your report should describe how your codes implement the homework (1-2 pages).  Below is an example for you. You do not necessarily follow the sample.

" Part 1: Read in the data file. Find the strings that 'xxx'. Use the command 'xxx' to do 'xxx'. The output format for each IP address is 'xxx'.

  Part 2: Do part 1 again, and run 'xxx' command to 'xxx'.

  ..."

  For IP address like "query2.lycos.cs.cmu.edu", you can aggregate them by the first two words, i.e. "query2.lycos.*.*". You do not necessarily to ping to find their IPs.
  




The default trigger for a PCollection is based on event time, and emits the results of the window when the Beam’s watermark passes the end of the window, and then fires each time late data arrives. The AfterWatermark trigger operates on event time. The AfterWatermark trigger emits the contents of a window after the watermark passes the end of the window, based on the timestamps attached to the data elements. The watermark is a global progress metric, and is Beam’s notion of input completeness within your pipeline at any given point. AfterWatermark only fires when the watermark passes the end of the window.

If unspecified, the default behavior is to trigger first when the watermark passes the end of the window, and then trigger again every time there is late arriving data.


"<number of bytes> bytes are served to < Aggregated IP address> in the hour period< window label>"

Alternative return [{'IP': rawOutput[0],
             'Size': rawOutput[1],
             'window_start':window_start,
             'window_end':window_end}]

" 4396 bytes are served to 111.111.111.* in the hour period from 00:00:00:00 to 00:00:59:59"


## Data:
epa-html.txt from http://ita.ee.lbl.gov/
-- Data is not included in push (too large)


 ## References:
 1. https://beam.apache.org/documentation/programming-guide/
 2. https://beam.apache.org/documentation/sdks/python/
 3. https://beam.apache.org/get-started/wordcount-example/
 4. http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html 
