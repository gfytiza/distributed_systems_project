import subprocess
import sys
import time

if (sys.argv[1] == 'insert'):
    count = 0
    with open('insert.txt') as f:
        mylist = f.read().splitlines()
    start_time = time.time()
    for line in mylist:
        count = count + 1
        print(line.split(',')[0])
        print(line.split(',')[1])
        bashCmd = ["python3", "cli.py", "insert", str(line.split(',')[0]), str(line.split(',')[1])]
        process = subprocess.Popen(bashCmd, stdout=subprocess.PIPE)

        output, error = process.communicate()
        print(output.decode("utf-8"))
    total_time = time.time()-start_time
    print('Write Throughput = ',total_time/count)
elif (sys.argv[1] == 'query'):
    count  = 0
    with open('query.txt') as f:
        mylist = f.read().splitlines()
    start_time = time.time()
    for line in mylist:
        count  = count +1
        print(line)
        bashCmd = ["python3", "cli.py", "query", str(line)]
        process = subprocess.Popen(bashCmd, stdout=subprocess.PIPE)
        output, error = process.communicate()
        print(output.decode("utf-8"))
    total_time = time.time()-start_time
    print('Read Throughput = ',total_time/count)

elif (sys.argv[1] == 'requests'):
    with open('requests.txt') as f:
        mylist = f.read().splitlines()
    start_time = time.time()
    for line in mylist:
        if ( line.split(',')[0] == 'insert'):
            print(line.split(',')[0])
            print(str(line.split(',')[1]))
            print(line.split(',')[2])
            bashCmd = ["python3", "cli.py", str(line.split(',')[0]), str(line.split(',')[1][1:]), str(line.split(',')[2])]
            process = subprocess.Popen(bashCmd, stdout=subprocess.PIPE)
            output, error = process.communicate()
            print(output.decode("utf-8"))
        else:
            print(line.split(',')[0])
            print(str(line.split(',')[1]))
            bashCmd = ["python3", "cli.py", str(line.split(',')[0]), str(line.split(',')[1][1:])]
            process = subprocess.Popen(bashCmd, stdout=subprocess.PIPE)
            output, error = process.communicate()
            print(output.decode("utf-8"))
    total_time = time.time()-start_time
    print('Time = ',total_time)
                                 