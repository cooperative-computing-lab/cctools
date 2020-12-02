import json
import sys
import os

#json_data='{"a":1, "b":2, 'c':3. "d":4, "e":5}'

#parsed_json = (json.loads(json_data))
#print(json.dumps(parsed_json, indent = 4, sort_keys=True)
 
 
#FILENAME = 'resource_output.summary'

#f = open(FILENAME, "r")

resource_output_json = json.load(sys.stdin)

#print(type(resource_output_json))



print('{}\t{}\t{}\t{}\t{}\t{}'.format(os.environ['scale'],
resource_output_json["wall_time"][0],
resource_output_json["memory"][0],
resource_output_json["cores_avg"][0],
resource_output_json["bytes_read"][0],
resource_output_json["bytes_written"][0]))
