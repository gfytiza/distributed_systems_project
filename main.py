import argparse
import requests
import json


nodes=['192.168.0.1:5000','192.168.0.1:5001','192.168.0.2:5001','192.168.0.3:5000','192.168.0.3:5001','192.168.0.4:5000','192.168.0.4:5001', '192.168.0.5:5000','192.168.0.5:5001']
#print(nodes)
nodes=['192.168.0.1:5000','192.168.0.1:5001']
#nodes=['192.168.0.1:5000']
for i in nodes:
        ploads = {'id':i}
        r = requests.get(url = 'http://192.168.0.2:5000/join', params=ploads)
