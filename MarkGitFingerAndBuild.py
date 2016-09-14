#!/usr/bin/python
# encoding=utf-8

import os
import sys

def GetGitFinger(repoPath):
    os.chdir(repoPath)
    print "Enter repo path:", os.getcwd()
    os.system("git log > tmp")
    with open('tmp','r') as f:
        l=f.readline()
        hash_code = l.split()[1]

    os.remove('tmp')
    print "Repo",repoPath,"'s git finger:",hash_code
    print # blank line
    return hash_code

def MarkBin():
    cwd = os.getcwd()
    print "make word in:", cwd
    
    start_path = cwd
    # 找出仓库的根目录
    found_root = False
    while True:
        for item in os.listdir(start_path):
            if item == '.git':
                found_root = True
                break
                
        if found_root:
            break
        os.chdir("..")
        start_path=os.getcwd()
            
    print "Repo's root path:", start_path
    print # blank line

    last_commit='icebergRepo := "%s"' % GetGitFinger('.')

    os.chdir(cwd)

    return cwd.split('/')[-1]

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
