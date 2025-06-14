
import json


with open('file1.txt', 'r') as file:
    # Read the entire content of the file
    service_map1 = json.loads(file.read())

with open('file2.txt', 'r') as file:
    # Read the entire content of the file
    service_map2 = json.loads(file.read())

existing_traffic = {}
existing_connections = {}

for traffic in service_map1['traffic']:
    key = (traffic['from_type'], traffic['from_id'], traffic['to_type'], traffic['to_id'], traffic['to_port'])
    existing_traffic[key] = traffic

for connection in service_map1['connections']:
    key = (connection['from_type'], connection['from_id'], connection['to_type'], connection['to_id'], connection['to_port'])
    existing_connections[key] = connection

new_traffic = {}
new_connections = {}

for traffic in service_map2['traffic']:
    key = (traffic['from_type'], traffic['from_id'], traffic['to_type'], traffic['to_id'], traffic['to_port'])
    if key not in existing_traffic:
        new_traffic[key] = traffic

for connection in service_map2['connections']:
    key = (connection['from_type'], connection['from_id'], connection['to_type'], connection['to_id'], connection['to_port'])
    if key not in existing_connections:
        new_connections[key] = connection

for traffic_key, traffic in new_traffic.items():
    from_type = traffic['from_type']
    from_id = traffic['from_id']
    to_type = traffic['to_type']
    to_id = traffic['to_id']
    try:
        from_name = service_map2[from_type + 's'][from_id]['name']
    except KeyError:
        from_name = 'Unknown'
    try:
        to_name = service_map2[to_type + 's'][to_id]['name']
    except KeyError:
        to_name = 'Unknown'

    traffic['from_name'] = from_name
    traffic['to_name'] = to_name

for connection_key, connection in new_connections.items():
    from_type = connection['from_type']
    from_id = connection['from_id']
    to_type = connection['to_type']
    to_id = connection['to_id']
    try:
        from_name = service_map2[from_type + 's'][from_id]['name']
    except KeyError:
        from_name = 'Unknown'
    try:
        to_name = service_map2[to_type + 's'][to_id]['name']
    except KeyError:
        to_name = 'Unknown'

    connection['from_name'] = from_name
    connection['to_name'] = to_name

print('New traffic:')
print(json.dumps(list(new_traffic.values()), indent=2))
print('New connections:')
print(json.dumps(list(new_connections.values()), indent=2))
