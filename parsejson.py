import json
import sys
import math
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import MinuteLocator, SecondLocator, DateFormatter
import operator 
import datetime

class parseIt:
    def __init__(self,data):
        #json_string = '{"first_name": "sejal", "last_name": "chauhan"}'
        #parsed_json = json.loads(json_string)
        jobID = data['jobID']
        totalMaps = data['totalMaps']
        totalReduces = data['totalReduces']
        totalTasks = totalMaps + totalReduces
        mapTasks = data['mapTasks']
        
        map_rec_num = 0
        # datetime.datetime.fromtimestamp(1347517370)
        #first_start
        #last_end
        records = []
        for mapRecord in mapTasks:
            rec = {}
            taskID = mapRecord["taskID"]
            task_start = mapRecord["startTime"]
            rec["start"] = task_start
            task_end = mapRecord["finishTime"]
            rec["end"] = task_end
            rec["task"] = 1
            map_rec_num  = map_rec_num + 1
            records.append(rec)
            #print taskID, task_start, task_end
        
        # aggrgate for each timestamp 
        timestamp = {}
        for record in records:
            start_t = record['start']
            end_t = record['end']
            if start_t not in timestamp:
                timestamp[start_t] = 1
            else:
                timestamp[start_t] = timestamp[start_t] + 1

            #make it end-time inclusive
            if end_t not in timestamp:
                timestamp[end_t] = 0
            else:
                timestamp[end_t] = timestamp[end_t] + 0

        for t in timestamp:
            for rec in records:
                if t > rec['start'] and t < rec['end']:
                    timestamp[t] = timestamp[t] + 1

        #another pass to remove items after end_time 
        #for record in records:
        #    start_t = record['start']
        #    end_t = record['end']
        #    for t in timestamp:
        #        if t > end_t:
        #            timestamp[rec['start']] = timestamp[rec['start']] - 1
 
        graph_data = []
        sorted_data = sorted(timestamp.items(), key=operator.itemgetter(0))

        for t in sorted_data:
            graph_data.append([ datetime.datetime.fromtimestamp(t[0]/1000.0) , t[1]])
        plot_graph(graph_data)
         

def plot_graph(graph_data):
    minutes = MinuteLocator()
    seconds = SecondLocator()
    #print(graph_data)
    time_data = [q[0] for q in graph_data]
    task_data = [q[1] for q in graph_data]
     
    fig, ax = plt.subplots()
    fig.suptitle('Acive tasks at a given time', fontsize=14)
    ax.plot_date(time_data, task_data, 'r-')
    
    #minsFmt = DateFormatter('%S')
    # format the ticks
    #ax.xaxis.set_major_locator(minutes)
    #ax.xaxis.set_major_formatter(minsFmt)
    #ax.xaxis.set_major_locator(seconds)
    ax.set_xlabel('Time')
    ax.set_ylabel('Num of active tasks at a given time')
    ax.autoscale_view()

    #ax.fmt_xdata = DateFormatter('%Y-%m-%d')
    #ax.fmt_ydata = price
    ax.grid(True)

    fig.autofmt_xdate()
    plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%M:%S"))
    plt.show()

if __name__ == '__main__':
    with open('job-trace.json') as data_file:
        data = json.load(data_file)
    parseIt(data)
