from flask import Flask, render_template, request
import requests
import hashlib
import json
import sys
from threading import Thread

app = Flask(__name__)
node_id='192.168.0.1:5000'
prev_id = None
next_id = None
first_node = False
data=[]
up=True
k = int(sys.argv[1])
consistency = sys.argv[2]

def hash (x):
        return hashlib.sha1(x.encode('utf-8')).hexdigest()

def  exist (data,key):
    for item in data :
        if item[0] == key:

            return (item)
def eventual_insert(ploads):
        r = requests.get(url = 'http://'+ next_id +'/chain_insert', params=ploads)
        return r.content.decode("utf-8")

def eventual_delete(ploads):
        r = requests.get(url = 'http://'+ next_id +'/chain_delete', params=ploads)
        return r.content.decode("utf-8")

def eventual_join(ploads):
        r = requests.get(url = 'http://'+ next_id + '/chain_join',params=ploads)
        return r.content.decode("utf-8")

@app.route('/insert',methods = ['GET'])
def result():
    if request.method == 'GET':
        x='insert'
        global data
        args = request.args
        print (args)
        hash_key = hash(args['key'])
        if (hash_key > hash(prev_id) and  hash_key <= hash(node_id) ) :
            x= exist(data, args['key'])
            if (x) :
                data.remove(x)
            data.append((args['key'],args['value']))
            if ( consistency == 'chain' and k != 1):
                ploads = {'key':args['key'], 'value':args['value'], 'k':k-1}
                r = requests.get(url = 'http://'+ next_id +'/chain_insert', params=ploads)
                print('wait for replicas')
                return r.content.decode("utf-8")
            if ( consistency == 'eventual' and k != 1):
                print("eventual")
                ploads = {'key':args['key'], 'value':args['value'], 'k':k-1}
                t = Thread(target=eventual_insert, args=(ploads, ))
                t.start()
                return 'eventual send'
            else:
                print(data)
                return "done by node "+ node_id
        elif ( (hash_key > hash(node_id) and first_node and hash_key > hash(prev_id)) or (hash_key<hash(prev_id) and first_node and hash_key <= hash(node_id)) ):
            x= exist(data, args['key'])
            if (x) :
                data.remove(x)
            data.append((args['key'],args['value']))
            if ( consistency == 'chain' and k != 1):
                ploads = {'key':args['key'], 'value':args['value'], 'k':k-1}
                r = requests.get(url = 'http://'+ next_id +'/chain_insert', params=ploads)
                print('wait for replicas')
                return r.content.decode("utf-8")
            if ( consistency == 'eventual' and k != 1):
                ploads = {'key':args['key'], 'value':args['value'], 'k':k-1}
                t = Thread(target=eventual_insert, args=(ploads, ))
                t.start()
                return 'eventual send'
            else:
                print ('done')
                print(data)
                return "done by node "+ node_id
        elif (hash_key > hash(node_id) or hash_key < hash(prev_id)):
            print('waiting...')
            ploads = {'key':args['key'], 'value':args['value']}
            r = requests.get(url = 'http://'+ next_id + '/insert',params=ploads)
            return r.content.decode("utf-8")
        print('insert from server')

@app.route('/delete',methods = ['GET'])
def result_delete():
    if request.method == 'GET':
        x='delete'
        args = request.args
        print (args)
        hash_key = hash(args['key'])
        if (hash_key > hash(prev_id) and  hash_key < hash(node_id) ) :
            x= exist(data, args['key'])
            if (x) :
                data.remove(x)
            if ( consistency == 'chain' and k != 1):
                ploads = {'key':args['key'], 'k':k-1}
                r = requests.get(url = 'http://'+ next_id +'/chain_delete', params=ploads)
                print('wait for replicas')
                return r.content.decode("utf-8")
            elif ( consistency == 'eventual' and k != 1):
                print("eventual")
                ploads = {'key':args['key'], 'k':k-1}
                t = Thread(target=eventual_delete, args=(ploads, ))
                t.start()
                return 'eventual send'
            else:
                print ('done')
                print(data)
                return "deleted by node "+ node_id
        elif ((hash_key > hash(node_id) and first_node and hash_key > hash(prev_id)) or (hash_key<hash(prev_id) and first_node and hash_key <= hash(node_id))):
            x= exist(data, args['key'])
            if (x) :
                data.remove(x)
            if ( consistency == 'chain' and k != 1):
                ploads = {'key':args['key'], 'k':k-1}
                r = requests.get(url = 'http://'+ next_id +'/chain_delete', params=ploads)
                print('wait for replicas')
                return r.content.decode("utf-8")
            elif ( consistency == 'eventual' and k != 1):
                print("eventual")
                ploads = {'key':args['key'], 'k':k-1}
                t = Thread(target=eventual_delete, args=(ploads, ))
                t.start()
                return 'eventual send'
            else:
                print ('done')
                print(data)
                return "deleted by node "+ node_id
        elif (hash_key > hash(node_id) or hash_key < hash(prev_id)):
            print('waiting...')
            ploads = {'key':args['key']}
            r = requests.get(url = 'http://'+ next_id + '/delete',params=ploads)
#            url ='http://'+ next_id + '/insert'
#            print(url)
            return r.content.decode("utf-8")

@app.route('/query',methods = ['GET'])
def result_query():
    if request.method == 'GET':
        x='query'
        args = request.args
        if ( up == False):
            return 'down'
        if (args['key'] != '*' and consistency == 'eventual'):
           x = exist(data, args['key'])
           if ( x ) :
               return json.dumps(x)
           else:
               ploads = {'key': args['key']}
               r = requests.get(url = 'http://'+ next_id + '/query', params=ploads)
               return r.content.decode("utf-8")
        if (args['key'] == '*'):
            return (json.dumps({'data': data}))
            # print('query from server')
        else :
            hash_key = hash(args['key'])
            if (hash_key > hash(prev_id) and  hash_key < hash(node_id) ) :
                x= exist(data, args['key'])
                if ( k != 1):
                    ploads = {'key':args['key'], 'k':k-1}
                    r = requests.get(url = 'http://'+ next_id +'/chain_query', params=ploads)
                    print('wait for replicas')
                    return r.content.decode("utf-8")
                else:
                    print ('done')
                    print(x)
                    return json.dumps(x)
            elif ((hash_key > hash(node_id) and first_node and hash_key > hash(prev_id)) or (hash_key<hash(prev_id) and first_node and hash_key <= hash(node_id))):
                x= exist(data, args['key'])
                if ( k != 1):
                    ploads = {'key':args['key'], 'k':k-1}
                    r = requests.get(url = 'http://'+ next_id +'/chain_query', params=ploads)
                    print('wait for replicas')
                    return r.content.decode("utf-8")
                else:
                    print ('done')
                    print(x)
                    return json.dumps(x)
            elif (hash_key > hash(node_id) or hash_key < hash(prev_id)):
                print('waiting...')
                ploads = {'key':args['key']}
                r = requests.get(url = 'http://'+ next_id + '/query',params=ploads)
                return r.content.decode("utf-8")

@app.route('/join',methods = ['GET'])
def result_join():
    global next_id, prev_id
    if request.method == 'GET':
        x='join'
        args = request.args
        hashed_id = hash(args['id'])
        if(next_id==None  and prev_id==None):
            next_id = args['id']
            prev_id = args['id']
            ploads = {'prev':node_id,'next':node_id}
            r = requests.get(url = 'http://'+ next_id + '/update',params=ploads)
            #return r.content.decode("utf-8")
            send_list=[]
            if (k!=1):
                for i in data:
                    ploads = {'key':i[0], 'value':i[1]}
                    r = requests.get(url = 'http://'+ next_id + '/aux_insert',params=ploads)
            else:
                for i in data:
                    if (hash(i[0]) <= hashed_id or hash(i[0])>= hash(node_id)):
                        send_list.append(i)
                        data.remove(i)
                        for i in send_list:
                            ploads = {'key':i[0], 'value':i[1]}
                            r = requests.get(url = 'http://'+ next_id + '/insert',params=ploads)
        elif((hashed_id < hash(node_id) and hashed_id > hash(prev_id)) or (hashed_id < hash(node_id) and hashed_id < hash(prev_id) and hash(prev_id)>hash(node_id)) or (hashed_id > hash(node_id) and hashed_id > hash(prev_id) and hash(prev_id)>hash(node_id)) ):
            old_prev = prev_id
            prev_id = args['id']
            print( 'Previous = ',prev_id, 'Next = ',next_id)
            ploads = {'prev':old_prev,'next':node_id}
            r = requests.get(url = 'http://'+ prev_id + '/update',params=ploads)
            ploads = {'prev':'n','next':prev_id}
            r = requests.get(url = 'http://'+ old_prev + '/update',params=ploads)
            send_list=[]
            if (k!=1 and consistency == 'chain'):
                for i,v in data:
                    if (hash(i) <= hashed_id or hash(i)>= hash(node_id)):
                        send_list.append((i,v))
                        data.remove((i,v))
                for i in send_list:
                    ploads = {'key':i[0], 'value':i[1]}
                    r = requests.get(url = 'http://'+ prev_id + '/aux_insert',params=ploads)
                ploads = {'k':k-1}
                r = requests.get(url = 'http://'+ next_id + '/chain_join',params=ploads)
            if (k!=1 and consistency=='eventual'):
                for i,v in data:
                    if (hash(i) <= hashed_id or hash(i)>= hash(node_id)):
                        send_list.append((i,v))
                        data.remove((i,v))
                for i in send_list:
                    ploads = {'key':i[0], 'value':i[1]}
                    r = requests.get(url = 'http://'+ prev_id + '/aux_insert',params=ploads)
                ploads = {'k':k-1}
                t = Thread(target=eventual_join, args=(ploads, ))
                t.start()
            else:
                for i,v in data:
                    if (hash(i) <= hashed_id or hash(i)>= hash(node_id)):
                        send_list.append((i,v))
                        data.remove((i,v))
                for i in send_list:
                    ploads = {'key':i[0], 'value':i[1]}
                    r = requests.get(url = 'http://'+ prev_id + '/insert',params=ploads)
        else:
            r = requests.get(url = 'http://'+ prev_id + '/join',params=args)
            #return r.content.decode("utf-8")
        return r.content.decode("utf-8")
        print('join server')

@app.route('/update',methods = ['GET'])
def result_update():
    global next_id, prev_id
    print(prev_id)
    if request.method == 'GET':
        args = request.args
        if (args['prev']!='n'): prev_id=args['prev']
        if (args['next']!='n'): next_id=args['next']
    print( 'Previous = ',prev_id, 'Next = ',next_id)
    return (prev_id)

@app.route('/depart',methods = ['GET'])
def result_depart():
    global data
    global next_id,prev_id
    args = request.args
    print (args)
    hashed_id = hash(args['id'])
    ploads = {'prev':prev_id,'next':'n'}
    r = requests.get(url = 'http://'+ next_id + '/update',params=ploads)
    ploads = {'prev':'n','next':next_id}
    r = requests.get(url = 'http://'+ prev_id + '/update',params=ploads)
    send_list=[]
    send=[]
    print('Data: ', data)
    send_list.append(data[0])
    send_list.append(data[1])
    send_list.append(data[2])
    send_list.append(data[3])
    send_list.append(data[4])
    send_list.append(data[5])
#    send_list.append(data[6])
 #   send_list.append(data[7])
  #  send_list.append(data[8])
   # send_list.append(data[9])
    for i,v in data:
        print(i,v)
        send.append((i,v))
    data.clear()
    for i in send:
        ploads = {'key':i[0],'value':i[1]}
        r = requests.get(url = 'http://'+ next_id + '/insert',params=ploads)
        r = requests.get(url = 'http://'+ prev_id + '/aux_depart',params=ploads)
        r = requests.get(url = 'http://'+ next_id + '/aux_depart',params=ploads)
    return r.content.decode("utf-8")

@app.route('/overlay',methods = ['GET'])
def result_overlay():
    if request.method == 'GET':
        args = request.args
        l = args['top']
        count = int(args['count'])
        l = l+'->'+node_id
        if (count == 1):
            print(l)
            return l
        else:
            ploads = {'top':l,'count':count-1}
            r = requests.get(url = 'http://'+ next_id + '/overlay',params=ploads)
            return r.content.decode("utf-8")

@app.route('/chain_insert', methods = ['GET'])
def result_chain_insert():
    global data
    if request.method == 'GET':
        args = request.args
        x = exist(data, args['key'])
        if (x):
            data.remove(x)
        data.append((args['key'],args['value']))
        print(args['k'])
        if ( int(args['k']) == 1 ):
            print(data)
            return "chain replication done"
        else:
            print(data)
            ploads = {'key':args['key'], 'value':args['value'], 'k':int(args['k'])-1}
            r = requests.get(url = 'http://'+ next_id +'/chain_insert',params=ploads)
            return r.content.decode("utf-8")

@app.route('/chain_delete', methods = ['GET'])
def result_chain_delete():
    if request.method == 'GET':
        args = request.args
        x = exist(data, args['key'])
        if (x):
            data.remove(x)
        print(args['k'])
        if ( int(args['k']) == 1 ):
            print(data)
            return "deletion done"
        else:
            print(data)
            ploads = {'key':args['key'], 'k':int(args['k'])-1}
            r = requests.get(url = 'http://'+ next_id +'/chain_delete',params=ploads)
            return r.content.decode("utf-8")
@app.route('/aux_insert',methods = ['GET'])
def aux_result():
    if request.method == 'GET':
        args = request.args
        print (args)
        hash_key = hash(args['key'])
        x= exist(data, args['key'])
        if (x) :
            data.remove(x)
        data.append((args['key'],args['value']))
        return 'aux_insert'

@app.route('/chain_join', methods = ['GET'])
def result_chain_join():
    global next_id, prev_id
    if request.method == 'GET':
        send_list = []
        args = request.args
        if (int(args['k'])!=0):
            for i,v in data:
                if (hash(i) <= hash(prev_id) or hash(i)>= hash(node_id)):
                    send_list.append((i,v))
                    data.remove((i,v))
            for i in send_list:
                ploads = {'key':i[0], 'value':i[1]}
                r = requests.get(url = 'http://'+ prev_id + '/aux_insert',params=ploads)
            ploads = {'k':int(args['k'])-1}
            r = requests.get(url = 'http://'+ next_id + '/chain_join',params=ploads)
            return r.content.decode("utf-8")
        return 'chain join'
@app.route('/aux_depart',methods = ['GET'])
def result_aux_depart():
    if request.method == 'GET':
        for i in data:
            hash_key = hash(i[0])
            if (hash_key > hash(prev_id) and  hash_key <= hash(node_id) ) :
                if ( consistency == 'chain' and k != 1):
                    ploads = {'key':i[0], 'value':i[1], 'k':k-1}
                    r = requests.get(url = 'http://'+ next_id +'/chain_insert', params=ploads)
                    return r.content.decode("utf-8")
                if ( consistency == 'eventual' and k != 1):
                    print("eventual")
                    ploads = {'key':i[0], 'value':i[1], 'k':k-1}
                    t = Thread(target=eventual_insert, args=(ploads, ))
                    t.start()
                    return 'eventual send'
            elif ( (hash_key > hash(node_id) and first_node and hash_key > hash(prev_id)) or (hash_key<hash(prev_id) and first_node and hash_key <= hash(node_id)) ):
                if ( consistency == 'chain' and k != 1):
                    ploads = {'key':i[0], 'value':i[1], 'k':k-1}
                    r = requests.get(url = 'http://'+ next_id +'/chain_insert', params=ploads)
                    return r.content.decode("utf-8")
                if ( consistency == 'eventual' and k != 1):
                    ploads = {'key':i[0], 'value':i[1], 'k':k-1}
                    t = Thread(target=eventual_insert, args=(ploads, ))
                    t.start()
                    return 'eventual send'
        return 'aux depart'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000,debug = True)
