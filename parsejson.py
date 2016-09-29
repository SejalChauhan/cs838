import json
import sys
import math

class parseIt:
    def __init__(self,data):
        #json_string = '{"first_name": "sejal", "last_name": "chauhan"}'
        #parsed_json = json.loads(json_string)
        print (data['taskType'])
        print (data['totalMaps'])
        print (data['totalReduces'])
        print (data['reduceTasks'])

if __name__ == '__main__':
    with open('data.json') as data_file:
        data = json.load(data_file)
    parseIt(data)
