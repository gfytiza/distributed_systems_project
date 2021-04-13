import argparse
import requests
import json
import random

my_parser = argparse.ArgumentParser()
my_parser.add_argument('operation', action='store', help='Select operation: insert <key> <value>, delete <key>, query <key>, depart, overlay, help' )

my_parser.add_argument('arguments', action='store', help='Give arguments if needed', nargs=argparse.REMAINDER)

args = my_parser.parse_args()

nodes=['192.168.0.1:5000','192.168.0.1:5001','192.168.0.2:5000','192.168.0.2:5001','192.168.0.3:5000','192.168.0.3:5001','192.168.0.4:5000','192.168.0.4:5001', '192.168.0.5:5000','192.168.0.5:5001']
nodes=['192.168.0.1:5000', '192.168.0.1:5001','192.168.0.2:5000']
#nodes=['192.168.0.1:5001','192.168.0.2:5000']
ip=random.choice(nodes)

if (args.operation == 'insert') :
    if (not args.arguments or len(args.arguments) != 2):
        print('Give arguments: <key> <value>.')
    elif (len(args.arguments) == 2 ) :
        ploads = {'key':args.arguments[0],'value':args.arguments[1]}
        r = requests.get(url = 'http://'+ ip +'/insert', params=ploads)
        print('Request send from: '+ip)
        print(r.content.decode("utf-8"))
elif (args.operation == 'delete'):
    if (not args.arguments or len(args.arguments) != 1):
        print("Give argument: <key>.")
    else:
        print('delete')
        ploads = {'key':args.arguments[0]}
        r = requests.get(url = 'http://' + ip + '/delete', params=ploads)
        print(r.content.decode("utf-8"))
elif (args.operation == 'query'):
    if (not args.arguments or len(args.arguments) != 1):
        print("Give argument: <key> or *.")
    else:
        ploads = {'key':args.arguments[0]}
        if (args.arguments[0] == "*"):
            for node in nodes:
                r = requests.get(url = 'http://'+ node +'/query', params=ploads)
                if (r.content.decode("utf-8") != 'down') :
                        print(node)
                        data = json.loads(r.content)['data']
                        for i in data:
                                print(i)
        else :
            r = requests.get(url = 'http://'+ ip +'/query', params=ploads)
            print('Request send from: '+ip)
            print(r.content.decode("utf-8"))
elif (args.operation == 'depart'):
    if (args.arguments):
        print('depart')
        ploads = {'id':args.arguments}
        node = args.arguments[0]
        r = requests.get(url = 'http://'+ node +'/depart', params=ploads)
        print(r.url)
        print(r.status_code)
    else:
        print(' Give node_id')
elif (args.operation == 'overlay'):
    if (not args.arguments):
        print('overlay')
        ploads = {'top':'Overlay : ', 'count':len(nodes)}
        r = requests.get(url = 'http://'+ ip +'/overlay', params=ploads)
        overlay = r.content.decode("utf-8")
        print(overlay)
elif (args.operation == 'help'):
    if (not args.arguments):
        print('insert <key> <value>: insert a new (key,value) pair.')
        print('delete <key>: delete a (key, value) pair with specific key.')
        print('depart: departure of the node.')
        print('overlay: print the topology of the network.')
    else:
        print('There are no arguments required.')
