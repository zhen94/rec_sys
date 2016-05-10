#encoding=utf-8
import json

def trans_type(line):
    try:
        line = eval(line)
    except:
        line = None
    return line

def exam(line):
    if line == None:
        return False
    if 'items' not in line:
        return False
    if 'user_id' not in line:
        return False
    if 'total_num' not in line:
        return False
    return True

def main():
    data = open('./paser_log', 'rb').read()
    data = data.split('\n')
    data = map(trans_type, data)
    data = filter(exam, data)
    data = map(json.dumps, data)
    out_fp = open('./data', 'wb')
    out_fp.write('\n'.join(data))

if __name__ == '__main__':
    main()
