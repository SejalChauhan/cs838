import json
import sys
import math
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.dates import MinuteLocator, SecondLocator, DateFormatter
from matplotlib.ticker import MaxNLocator

import codecs
import unicodedata

import operator 
import datetime
import numpy as np
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
from mpl_toolkits.axes_grid1.inset_locator import mark_inset


class parseIt:
    def __init__(self,data):
        #json_string = '{"first_name": "sejal", "last_name": "chauhan"}'
        #parsed_json = json.loads(json_string)
        maps = {}
        reduces = {}
        for record in data:
           if record['entitytype'] == "TEZ_DAG_ID" and contains_mappings(record):
                otherinfo = record['otherinfo']
                mapping = otherinfo["vertexNameIdMapping"]
                for u_task, val in mapping.items():
                    task = str(u_task)
                    if task.startswith("Map") or task.startswith("map"):
                        maps[val] = {}
                    else:
                        reduces[val] = {}
                #populated maps and reduces 
        
        for record in data:
            for vertex_id in maps:
                if record['entity'] == vertex_id and contains_start_time(record):
                    otherinfo = record["otherinfo"]
                    maps[vertex_id]['startTime'] = otherinfo['startTime']

                if record['entity'] == vertex_id and contains_end_time(record):
                    otherinfo = record["otherinfo"]
                    maps[vertex_id]['endTime'] = otherinfo['endTime']
                    maps[vertex_id]['timeTaken'] = otherinfo['timeTaken']
             
            for vertex_id in reduces:
                if record['entity'] == vertex_id and contains_start_time(record):
                    otherinfo = record["otherinfo"]
                    reduces[vertex_id]['startTime'] = otherinfo['startTime']
                 
                if record['entity'] == vertex_id and contains_end_time(record):
                    otherinfo = record["otherinfo"]
                    reduces[vertex_id]['endTime'] = otherinfo['endTime']
                    reduces[vertex_id]['timeTaken'] = otherinfo['timeTaken']

        #jobID = data['jobID']
        totalMaps = len(maps)
        totalReduces = len(reduces)
        totalTasks = totalMaps + totalReduces
        
        self.total_maps = totalMaps
        self.total_reduces = totalReduces
        self.total_tasks = totalTasks 
        
        self.map_data = parse_tasks(maps)
        self.reduce_data = parse_tasks(reduces)


def contains_start_time(record):
    if record['entitytype'] != "TEZ_VERTEX_ID":
        return False
    if "events" not in record:
        return False
    events = record["events"]
    if len(events) == 0:
        return False
    if events[0]["eventtype"] == "VERTEX_STARTED":
        return True
    return False 

def contains_end_time(record):
    if record['entitytype'] != "TEZ_VERTEX_ID":
        return False
    if "events" not in record:
        return False
    events = record["events"]
    if len(events) == 0:
        return False
    if events[0]["eventtype"] == "VERTEX_FINISHED":
        return True
    return False 


def contains_mappings(record):
    if "otherinfo" not in record:
        return False
    otherinfo = record['otherinfo']
    if "vertexNameIdMapping" not in otherinfo:
        return False
    return True

#It parses both map and reduce tasks 
def parse_tasks(tasks):
    records = []
    for vertex,record in tasks.items():
        rec = {}
        task_start = record["startTime"]
        rec["start"] = task_start
        task_end = record["endTime"]
        rec["end"] = task_end
        rec["task"] = 1
        records.append(rec)
    
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

    map_data = []
    sorted_map_data = sorted(timestamp.items(), key=operator.itemgetter(0))

    for t in sorted_map_data:
        #map_data.append([ datetime.datetime.fromtimestamp(t[0]/1000.0) , t[1]])
        map_data.append([t[0] , t[1]])
    return map_data


def plot_graph(map_data, reduce_data):
    minutes = MinuteLocator()
    seconds = SecondLocator()
    #print(graph_data)
    #time_data = np.array([q[0] for q in graph_data])
    map_time_data = [q[0] for q in map_data]
    min_map = min(map_time_data)
    reduce_time_data = [q[0] for q in reduce_data]
    min_reduce = min(reduce_time_data)
    min_overall = min(min_map, min_reduce)
    
    map_x = map(lambda x: (x - min_overall)/1000.0, map_time_data)
    #map_time_data = [q[0] for q in map_data]
    map_task_data = [q[1] for q in map_data]

    reduce_x = map(lambda x: (x - min_overall)/1000.0, reduce_time_data)
    reduce_task_data = [q[1] for q in reduce_data]

    #diff = time_data.max() - time_data.min()
    #print diff
    #x_axis = np.arange(diff) 
    #print x_axis
    fig, ax = plt.subplots(figsize=[25,10])
    fig.suptitle('Acive tasks at a given time', fontsize=14)
    #ax.plot_date(map_x, map_task_data, '-')
    ax.plot(map_x, map_task_data, '-')
    #ax.plot_date(reduce_x, reduce_task_data, 'r-')
    ax.plot(reduce_x, reduce_task_data, 'r-')
    
    #minsFmt = DateFormatter('%S')
    # format the ticks
    #ax.xaxis.set_major_locator(minutes)
    #ax.xaxis.set_major_formatter(minsFmt)
    #ax.xaxis.set_major_locator(seconds)
    ax.set_xlabel('Seconds since start')
    ax.set_ylabel('Num of tasks')
    #ax.autoscale_view()
    ax.grid(True)
    plt.legend(['Map Tasks', 'Reduce Taks'], loc='upper right')

    #ax.fmt_xdata = DateFormatter('%Y-%m-%d')
    #ax.fmt_ydata = price
    # Zoom in 
    axins = zoomed_inset_axes(ax, 0.4, loc=5) # zoom = 6
    axins.plot(map(lambda x: x*1000, map_x), map_task_data, '-')
    axins.plot(map(lambda x: x*1000, reduce_x), reduce_task_data, 'r-')
    #axins.invert_yaxis()
    #axins.axis([1, 5000, 1, 35])
    axins.set_xlim(0, 15)
    axins.set_ylim(0, 4)
    axins.xaxis.tick_top()
    axins.xaxis.set_major_locator(MaxNLocator(nbins=1, prune='lower'))
    axins.set_xlabel("Zoom in the first Sec")
    #axins.autoscale_view()
    #axins.imshow(Z2, extent=extent, interpolation="nearest", origin="lower")
    #mark_inset(ax, axins, loc1=2, loc2=4, fc="none", ec="0.5")

    #fig.autofmt_xdate()
    #plt.gca().xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M:%S"))
    plt.xticks(visible=False)
    #plt.yticks(visible=False)
    plt.show()

if __name__ == '__main__':
    json_file = sys.argv[1]
    total_maps = 0
    total_reduces = 0
    total_tasks = 0
    map_data = []
    reduce_data = []
    
    data_file = codecs.open(json_file, 'r', encoding='ISO-8859-1')
    data = json.load(data_file)
    parser = parseIt(data)
    total_maps = total_maps + parser.total_maps
    total_reduces = total_reduces + parser.total_reduces
    map_data.extend(parser.map_data)
    reduce_data.extend(parser.reduce_data)
    total_tasks = total_maps + total_reduces    
    print total_maps, total_reduces, total_tasks
    plot_graph(map_data, reduce_data)

