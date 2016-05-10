import os
import json

def main():
    path = './20160419'
    data = open(path, 'rb').read()
    data = json.loads(data)
    print len(data)

if __name__ == '__main__':
    main()
