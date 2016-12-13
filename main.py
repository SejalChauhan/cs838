#!/usr/bin/python

import os
#from pprint import pprint
import sys
import linecache
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict

from spark_events_parser.spark_run import SparkRun


def parse_dir(path):
    job_time_with_mlt = OrderedDict()
    job_time_with_quantile = {}

    for root, subdirs, files in os.walk(path):
        for subdirname in subdirs:
            parse_dir(os.path.join(root,subdirname))

        for filename in files:
            try:
                print filename
                parser = SparkRun(os.path.join(root, filename))
            except ValueError:
                continue
            except:
                print("Unexpected parse error while processing file: {}".format(filename), sys.exc_info()[0])
                continue
            try:
                stage_info = {}
                total_speculative_tasks = 0
                total_gc_time = 0
                total_run_time = 0
                total_ex_run_time = 0

                parser.correlate()
                report = parser.generate_report()
                # for t in parser.tasks.values():
                #parser.executors
                for j in parser.jobs.values():
                    ### Get stage specific stragglers and GC time ### 
                    for stage in j.stages:
                        #print("stage id: {}".format(stage.stage_id))
                        total_run_time = j.runtime + total_run_time
                        stage_info[stage.stage_id] = []
                        stage_info[stage.stage_id].append(float(stage.runtime) / 1000)
                        for task in stage.tasks:
                            total_ex_run_time = total_ex_run_time + task.runtime
                            if task.jvm_gc_time:
                                total_gc_time = total_gc_time + task.jvm_gc_time
                            if task.speculative:
                                total_speculative_tasks = total_speculative_tasks + 1
                        stage_info[stage.stage_id].append(float(total_ex_run_time) / 1000)
                        stage_info[stage.stage_id].append(total_speculative_tasks)
                        stage_info[stage.stage_id].append(float(total_gc_time) / 1000)
                        #print("\t info: {}".format(stage_info[stage.stage_id]))
                    #####generate graphs for stage#####
                    #sort by key
                    stage_vals = OrderedDict(sorted(stage_info.items(), key=lambda t: t[0]))  
                    #runtime
                    #generate_stage_graph(stage_vals.keys(), stage_vals.values(), 0, "Stage runtime")
                    #generate_stage_graph(stage_vals.keys(), stage_vals.values(), 1, "Stage runtime")
                    #jvm time
                    #generate_stage_graph(stage_vals.keys(), stage_vals.values(), 2, "Per stage straggler")
                    # num of stragglers
                    #generate_stage_graph(stage_vals.keys(), stage_vals.values(), 3, "Per stage JVM time")

                    if parser.parsed_data['speculation']:
                        x_val = ""
                        if float(parser.parsed_data['speculation_multiplier']) == 0 and float(parser.parsed_data['speculation_quantile']) == 0:
                            x_val = "no speculation"
                        else:
                            x_val = "m=" + str(parser.parsed_data['speculation_multiplier']) + ", q=" + str(parser.parsed_data['speculation_quantile'])
                        #job_time_with_mlt[x_val] = float(j.runtime) / 1000
                        # This is for stage_specific info

                        #job_time_with_mlt[float(parser.parsed_data['speculation_multiplier'])] = float(j.runtime) / 1000
                        #job_time_with_quantile[float(parser.parsed_data['speculation_quantile'])] = float(j.runtime) / 1000
                    else:
                        x_val = "no speculation"
                        #job_time_with_mlt[x_val] = float(j.runtime) / 1000
                        #job_time_with_mlt[0] = float(j.runtime) / 1000
                        #job_time_with_quantile[0] = float(j.runtime) / 1000
                
                tasks_data = OrderedDict()
                for t in parser.tasks.values():
                    #if t.speculative:
                    tasks_data[t.task_id] = float(t.runtime) / 1000
                #generate_multiplier_graph(tasks_data, filename, parser.parsed_data['speculation_multiplier'])
                #generate_quantile_graph(tasks_data)
                open(os.path.join(root, parser.get_app_name() + ".txt"), "w").write(report)
                print filename, float(total_run_time) / 1000, float(total_gc_time) / 1000 , total_speculative_tasks 
            except:
                print("Unexpected correlate error while processing file: {}".format(filename), sys.exc_info()[0])
                PrintException()
                continue

    #move it to the beginning 
    #jobs_run_time = OrderedDict(sorted(job_time_with_mlt.items(), key=lambda t: t[0]))  
    #jobs_with_stress = OrderedDict([('m=1.05, q=0.5', 204.275), ('m=1.05, q=0.75', 214.036), ('m=1.5, q=0.5', 207.964), ('m=1.5, q=0.75', 227.959), ('m=1.75, q=0.5', 226.968), ('m=1.75, q=0.75', 203.504), ('no speculation', 282.381)]) 
    #print jobs_run_time            
    #generate_multiplier_graph(jobs_run_time, " total query time", 0)
    #generate_compare_graph(jobs_run_time, jobs_with_stress)
    #generate_quantile_graph(job_time_with_mlt)

def PrintException():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)


def generate_stage_graph(x_keys, raw_data, index, title_str):
    #print data
    x_vals = x_keys
    data = [q[index] for q in raw_data]
    y_vals = np.fromiter(iter(data), dtype=float)
    print x_vals, y_vals

    #x_vals = [q[0] for q in x_]
    ind = np.arange(len(data))
    fig, ax = plt.subplots(figsize=[8, 6])
    width = 0.35
    rects = ax.bar(ind, y_vals, width, color ='orange')
    #labels and titles
    ax.set_ylabel("Time in seconds")
    ax.set_xlabel("Stage Id")
    ax.set_title("TPC-DS Q50: " + title_str)
    ax.set_xticks(ind + width)
    ax.set_xticklabels(x_vals)
    #plt.subplots_adjust(bottom=0.38)

    plt.show()

def generate_compare_graph(data, comp_data):
    #print data
    x_vals = data.keys() #np.fromiter(iter(data.keys()), dtype=float)
    comp_y_vals = np.fromiter(iter(comp_data.values()), dtype=float)
    y_vals = np.fromiter(iter(data.values()), dtype=float)
    print y_vals

    #x_vals = [q[0] for q in x_]
    ind = np.arange(len(data))
    fig, ax = plt.subplots(figsize=[8, 6])
    width = 0.35
    rects = ax.bar(ind, y_vals, width, color ='orange', label='without stress')
    rects2 = ax.bar(ind + width, comp_y_vals, width, color ='purple', label='with stress')
    #labels and titles
    ax.set_ylabel("Query runtime in seconds")
    ax.set_xlabel("speculative multipliers (m) and quantiles (q)")
    ax.set_title("Performance of TPC-DS Q50")
    ax.set_xticks(ind + width + width)
    ax.set_xticklabels(x_vals, rotation=-30)
    plt.legend(loc='upper left')
    plt.subplots_adjust(bottom=0.20)

    plt.show()

def generate_multiplier_graph(data, title_str, mlt_val):
    #print data
    x_vals = data.keys() #np.fromiter(iter(data.keys()), dtype=float)
    y_vals = np.fromiter(iter(data.values()), dtype=float)
    print y_vals

    #x_vals = [q[0] for q in x_]
    ind = np.arange(len(data))
    fig, ax = plt.subplots(figsize=[6, 4])
    width = 0.35
    rects = ax.bar(ind, y_vals, width, color ='orange')
    #labels and titles
    ax.set_ylabel("Query runtime in seconds")
    ax.set_xlabel("speculative multipliers (m) and quantiles (q)")
    ax.set_title("Baseline performance for TPC-DS Q50 ")
    ax.set_xticks(ind + width)
    ax.set_xticklabels(x_vals, rotation=-50)
    plt.subplots_adjust(bottom=0.38)

    plt.show()

def generate_quantile_graph(data):
    #print data
    x_vals = np.fromiter(iter(data.keys()), dtype=float)
    y_vals = np.fromiter(iter(data.values()), dtype=float)

    ind = np.arange(len(data))
    fig, ax = plt.subplots()
    width = 0.1
    rects = ax.bar(ind, y_vals, width, color ='orange')
    #labels and titles
    ax.set_ylabel("Runtime in seconds")
    ax.set_xlabel("Tasks as they are created")
    ax.set_title("Distribution of tasks and their completion time")
    ax.set_xticks(ind + width)
    ax.set_xticklabels(x_vals)

    plt.show()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        PATH = os.path.join("app_data")
    else:
        PATH = os.path.join(sys.argv[1])
    parse_dir(PATH)
