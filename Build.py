#!/usr/bin/python
# encoding=utf-8

import os
import sys

def MarkBin():
    return "sqlregret"

def GoBuild(platform, targetName):
    targetName+=".exe"
    if platform == 'linux':
        os.system('''GOOS=linux GOARCH=amd64 go build -o %s''' % targetName)
    elif sys.platform == 'darwin':
        os.system('''go build -o %s''' % targetName)
    else:
        os.system('''GOOS=linux GOARCH=amd64 go build -o %s''' % targetName)

    print "taget name:", targetName


if __name__=="__main__":
    projName = MarkBin()

    if len(sys.argv) >= 2:
        GoBuild(sys.argv[-1], projName)
    else:
        GoBuild("", projName)
