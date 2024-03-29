from datetime import datetime

from numpy import *

from executor import Executor
from job import Job
from block_manager import BlockManager
from task import Task
from spark_events_parser.utils import get_json

# added 
import json 

class SparkRun:
    def __init__(self, filename):
        """
        :param filename: str
        """
        self.filename = filename
        self.parsed_data = {}
        self.executors = {}
        self.jobs = {}
        self.tasks = {}
        self.block_managers = []

        f = open(filename, "r")

        for line in f:
            json_data = get_json(line)
            event_type = json_data["Event"]
            if event_type == "SparkListenerLogStart":
                self.do_SparkListenerLogStart(json_data)
            elif event_type == "SparkListenerBlockManagerAdded":
                self.do_SparkListenerBlockManagerAdded(json_data)
            elif event_type == "SparkListenerEnvironmentUpdate":
                self.do_SparkListenerEnvironmentUpdate(json_data)
            elif event_type == "SparkListenerApplicationStart":
                self.do_SparkListenerApplicationStart(json_data)
            elif event_type == "SparkListenerJobStart":
                self.do_SparkListenerJobStart(json_data)
            elif event_type == "SparkListenerStageSubmitted":
                self.do_SparkListenerStageSubmitted(json_data)
            elif event_type == "SparkListenerExecutorAdded":
                self.do_SparkListenerExecutorAdded(json_data)
            elif event_type == "SparkListenerTaskStart":
                self.do_SparkListenerTaskStart(json_data)
            elif event_type == "SparkListenerTaskEnd":
                self.do_SparkListenerTaskEnd(json_data)
            elif event_type == "SparkListenerExecutorRemoved":
                self.do_SparkListenerExecutorRemoved(json_data)
            elif event_type == "SparkListenerBlockManagerRemoved":
                self.do_SparkListenerBlockManagerRemoved(json_data)
            elif event_type == "SparkListenerStageCompleted":
                self.do_SparkListenerStageCompleted(json_data)
            elif event_type == "SparkListenerJobEnd":
                self.do_SparkListenerJobEnd(json_data)
            else:
                print("WARNING: unknown event type: " + event_type)

    def do_SparkListenerLogStart(self, data):
        self.parsed_data["spark_version"] = data["Spark Version"]

    def do_SparkListenerBlockManagerAdded(self, data):
        bm = BlockManager(data)
        self.block_managers.append(bm)

    def do_SparkListenerEnvironmentUpdate(self, data):
        self.parsed_data["java_version"] = data["JVM Information"]["Java Version"]
        self.parsed_data["app_name"] = data["Spark Properties"]["spark.app.name"]
        self.parsed_data["app_id"] = data["Spark Properties"]["spark.app.id"]

        #speculation stuff
        if "spark.speculation" in data["Spark Properties"] :
            self.parsed_data["speculation"] = data["Spark Properties"]["spark.speculation"]
        else:
            self.parsed_data["speculation"] = False 
        if "spark.speculation.multiplier" in data["Spark Properties"] :
            self.parsed_data["speculation_multiplier"] = data["Spark Properties"]["spark.speculation.multiplier"]
        else:
            self.parsed_data["speculation_multiplier"] = 0
        if "spark.speculation.quantile" in data["Spark Properties"] :
            self.parsed_data["speculation_quantile"] = data["Spark Properties"]["spark.speculation.quantile"]
        else:
            self.parsed_data["speculation_quantile"] = 0

        if "spark.driver.memory" in data["Spark Properties"] :
            self.parsed_data["driver_memory"] = data["Spark Properties"]["spark.driver.memory"]
        else:
            self.parsed_data["driver_memory"] = ""
        if "spark.executor.memory" in data["Spark Properties"]: 	
            self.parsed_data["executor_memory"] = data["Spark Properties"]["spark.executor.memory"]
        else:
            self.parsed_data["executor_memory"] = ""
        self.parsed_data["commandline"] = data["System Properties"]["sun.java.command"]

    def do_SparkListenerApplicationStart(self, data):
        self.parsed_data["app_start_timestamp"] = data["Timestamp"]

    def do_SparkListenerJobStart(self, data):
        job_id = data["Job ID"]
        if job_id in self.jobs:
            print("ERROR: Duplicate job ID!")
            return
        job = Job(data)
        self.jobs[job_id] = job

    def do_SparkListenerStageSubmitted(self, data):
        pass

    def do_SparkListenerExecutorAdded(self, data):
        exec_id = data["Executor ID"]
        self.executors[exec_id] = Executor(data)

    def do_SparkListenerTaskStart(self, data):
        task_id = data["Task Info"]["Task ID"]
        self.tasks[task_id] = Task(data)

    def do_SparkListenerTaskEnd(self, data):
        task_id = data["Task Info"]["Task ID"]
        self.tasks[task_id].finish(data)

    def do_SparkListenerExecutorRemoved(self, data):
        exec_id = data["Executor ID"]
        self.executors[exec_id].remove(data)

    def do_SparkListenerBlockManagerRemoved(self, data):
        pass

    def do_SparkListenerStageCompleted(self, data):
        stage_id = data["Stage Info"]["Stage ID"]
        for j in self.jobs.values():
            for s in j.stages:
                if s.stage_id == stage_id:
                    s.complete(data)

    def do_SparkListenerJobEnd(self, data):
        job_id = data["Job ID"]
        self.jobs[job_id].complete(data)

    def correlate(self):
        # Link block managers and executors
        for bm in self.block_managers:
            if  bm.executor_id in self.executors:
                if bm.executor_id != '<driver>':
                    self.executors[bm.executor_id].block_managers.append(bm)

        for t in self.tasks.values():
            self.executors[t.executor_id].tasks.append(t)
            for j in self.jobs.values():
                for s in j.stages:
                    if s.stage_id == t.stage_id:
                        s.tasks.append(t)

        self.parsed_data["num_failed_tasks"] = 0
        self.parsed_data["num_success_tasks"] = 0
        for t in self.tasks.values():
            if t.end_reason != "Success":
                self.parsed_data["num_failed_tasks"] += 1
            else:
                self.parsed_data["num_success_tasks"] += 1

        # Total average and stddev task run time
        all_runtimes = [ x.finish_time - x.launch_time for x in self.tasks.values() if x.end_reason == "Success" ]
        all_runtimes = array(all_runtimes)
        self.parsed_data["tot_avg_task_runtime"] = all_runtimes.mean()
        self.parsed_data["tot_std_task_runtime"] = all_runtimes.std()
        self.parsed_data["min_task_runtime"] = all_runtimes.min()
        self.parsed_data["max_task_runtime"] = all_runtimes.max()

    def generate_report(self):
        s = "Report for '{}' execution {}\n".format(self.parsed_data["app_name"], self.parsed_data["app_id"])
        s += "Spark version: {}\n".format(self.parsed_data["spark_version"])
        s += "Java version: {}\n".format(self.parsed_data["java_version"])
        s += "Start time: {}\n".format(datetime.fromtimestamp(self.parsed_data["app_start_timestamp"]/1000))
        s += "Commandline: {}\n\n".format(self.parsed_data["commandline"])
        s += "---> Jobs <---\n"
        for j in self.jobs.values():
            s += j.report(0)
            s += "\n"
        s += "---> Tasks <---\n"
        s += "Total tasks: {}\n".format(len(self.tasks))
        s += "Successful tasks: {}\n".format(self.parsed_data["num_success_tasks"])
        s += "Failed tasks: {}\n".format(self.parsed_data["num_failed_tasks"])
        s += "Task average runtime: {} ({} stddev)\n".format(self.parsed_data["tot_avg_task_runtime"], self.parsed_data["tot_std_task_runtime"])
        s += "Task min/max runtime: {} min, {} max\n".format(self.parsed_data["min_task_runtime"], self.parsed_data["max_task_runtime"])
        for t in self.tasks.values():
            s += t.report(0)
            s += "\n"

        s += "---> Executors <---\n"
        for e in self.executors.values():
            s += e.report(0)
            s += "\n"
#        s += "---> Block managers <---\n"
#        for bm in self.block_managers:
#            s += bm.report(0)
        return s

    def get_app_name(self):
        return self.parsed_data["app_id"]
