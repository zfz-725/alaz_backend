import json
import os
import sys
import django


sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()

from core.clickhouse_utils import get_clickhouse_client, insert_tcpdump
import random
import string
from datetime import datetime, UTC, timedelta

# Function to generate random strings
def random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

# Function to generate a random hostname
def random_hostname():
    tlds = [".com", ".net", ".org", ".io"]
    return random_string(7) + random.choice(tlds)

def generate_realistic_payload():
    # Randomize User-Agent
    user_agents = [
        "python-requests/2.29.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "curl/7.68.0",
    ]
    # Randomize Accept-Encoding
    accept_encodings = ["gzip, deflate, br", "deflate, gzip", "*"]
    # Randomize Content-Type
    content_types = ["application/json", "application/graphql"]
    http_methods = ["GET", "POST", "PUT", "DELETE"]
    # Extra headers
    languages = ["en-US,en;q=0.5", "en-GB,en;q=0.5", "de-DE,de;q=0.5"]

    # Construct headers with extra fields and random host
    headers = f"""{random.choice(http_methods)} /graphql HTTP/1.1
Host: {random_hostname()}:4000
User-Agent: {random.choice(user_agents)}
Accept-Encoding: {random.choice(accept_encodings)}
Accept-Language: {random.choice(languages)}
Accept: */*
Connection: keep-alive
Content-Type: {random.choice(content_types)}
X-Custom-Header: CustomValue
Content-Length: """

    # Generate a more complex GraphQL query body
    fields = ["id", "name", "email", "address", "phone", "company { name address }", "friends { name id }"]
    random.shuffle(fields)  # Shuffle to randomize the order of fields
    query_body = {"query": "{\n  user {\n    " + "\n    ".join(fields) + "\n  }\n}\n"}
    body_str = json.dumps(query_body, indent=2)  # Pretty print for a longer payload
    content_length = len(body_str.encode('utf-8'))  # Ensure correct length for multibyte chars

    # Combine headers and body
    http_request = headers + str(content_length) + "\n\n" + body_str

    return http_request

# # Generate realistic HTTP headers
# def generate_realistic_headers():
#     user_agents = [
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
#         "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
#         "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36",
#         # Add more user agents as needed
#     ]
#     accept_types = ["text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"]
#     content_types = ["application/json", "application/x-www-form-urlencoded"]
    
#     headers = {
#         "User-Agent": random.choice(user_agents),
#         "Accept": random.choice(accept_types),
#         "Content-Type": random.choice(content_types),
#         "Host": "example.com",
#         # Add more headers as needed
#     }
#     return json.dumps(headers)  # Convert the headers dictionary to a JSON string

# # Generate a realistic body
# def generate_realistic_body():
#     # Example for a JSON body
#     body_content = {
#         "name": random_string(10),
#         "email": f"{random_string(5)}@example.com",
#         "message": random_string(50)
#     }
#     return json.dumps(body_content)  # Convert the body content to a JSON string

# # Function to generate random timestamp
# def random_timestamp():
#     return int(time.time() * 1_000_000)

# Initialize ClickHouse client
client = get_clickhouse_client()
table = 'alaz.tcpdump'

# Number of rows to insert
rpm = 400_000
minutes = 60
batch_size = 10_000

# Prepare insert query
insert_query = '''
INSERT INTO alaz.tcpdump (
    timestamp, monitoring_id, cluster_id, 
    resource_type, resource_id, response_code, 
    response_time_ms, body, port, headers
) VALUES
'''

monitoring_id = 'monitoring_id_1'
cluster_id = 'cluster_id_1'
resource_type = 'resource_type_1'
resource_id = 'resource_id_1'

# Generate random data
data = []
for min in range(minutes):
    print(f'Inserting data for minute {min+1}/{minutes}')
    timestamp = int((datetime.now(UTC) - timedelta(minutes=minutes-min)).timestamp() * 1_000_000) # in microseconds
    for _ in range(rpm):
        from_ip = f'{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}'
        to_ip = f'{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}'
        from_port = random.randint(1, 65535)
        to_port = random.randint(1, 65535)
        payload = generate_realistic_payload()
        
        # Create a tuple with the random data
        row = (timestamp, monitoring_id, cluster_id, resource_type, resource_id, from_ip, to_ip, from_port, to_port, payload)
        data.append(row)
        
        if len(data) >= batch_size:
            insert_tcpdump(data, client)
            data = []

# Insert the remaining data
if len(data) > 0:
    insert_tcpdump(data, client)
    data = []

print(f'Inserted {rpm * minutes} rows into alaz.tcpdump table.')