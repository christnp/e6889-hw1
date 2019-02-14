#!/usr/bin/env python2

fname = "/home/christnp/Development/test.txt-00000-of-00001"

with open(fname) as f:
    content = f.readlines()

print(content)


