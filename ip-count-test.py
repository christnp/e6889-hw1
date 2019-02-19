#!/usr/bin/env python2

def parse(element):
    try:
      elements = element.split(" ")
    except ValueError:
      # keep track of how many times we fail to split/parse
      print(element)
      return [("Failed Parse",1)]
    
    ip = bytes(elements[0])
    # we have to make sure size is an integer, else set to 0
    try:
      size = int(elements[-1])
    except ValueError:
      # Handle the exception
      size = 0

    return [(ip,size)]

#fname = "/home/christnp/Development/e6889/e6889-hw1/NASA_access_log_Jul95"
fname = "/home/christnp/Development/e6889/e6889-hw1/NASA_access_log_Jul95_test"
ip_size = []
with open(fname) as f:
    content = f.readlines()
    #print(content[0])
    #print("\nnext\n)")
    #(ip,size) = parse(content)
    #ip_size.append([ip,size])

for line in content:
    #tmp = parse(line)
    tmp = line[len('['):-len(']')]
    print('%s\n',tmp)
    ip_size.append(tmp)

#print(ip_size)



