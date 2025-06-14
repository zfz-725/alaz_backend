import datetime
import socket
import time

# 1- No splitting, a new metadata comes
# 2- Splitting

def no_split(socket):
    # metadata1 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_f777de78-aa0d-45f4-b5db-4840cb1bb5dd_TammySweeney_2**\n'
    
    # partial_log1 = '2024-05-17T10:53:17.355087080+00:00 stdout P 1st message\n'
    # partial_log2 = '2024-05-17T10:53:17.355087080+00:00 stdout F 2nd message\n'

    # socket.sendall(metadata1.encode('utf-8'))
    # print(f'Sent metadata1: {metadata1}')
    # socket.sendall(partial_log1.encode('utf-8'))
    # print(f'Sent partial_log1: {partial_log1}')
    # socket.sendall(partial_log2.encode('utf-8'))
    # print(f'Sent partial_log2: {partial_log2}')
    
    metadata2 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_d920a7cc-8a82-40e3-aef5-e40c2d6d1dd9_TammySweeney_1**\n'
    
    # partial_log3 = '2024-05-17T10:53:17.355087080+00:00 stdout P 3rd message\n'
    # partial_log4 = '2024-05-17T10:53:17.355087080+00:00 stdout F 4th message\n'

    socket.sendall(metadata2.encode('utf-8'))
    print(f'Sent metadata2: {metadata2}')

    for i in range(500):
        now = datetime.datetime.now(datetime.timezone.utc)
        date_format = now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        log = f'{date_format} stdout P {i}\n'
        socket.sendall(log.encode('utf-8'))
        time.sleep(0.1)

    # socket.sendall(partial_log3.encode('utf-8'))
    # print(f'Sent partial_log3: {partial_log3}')
    # socket.sendall(partial_log4.encode('utf-8'))
    # print(f'Sent partial_log4: {partial_log4}')

    # print("Closing connection")
    

def split(socket):
    metadata1 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_f777de78-aa0d-45f4-b5db-4840cb1bb5dd_TammySweeney_1**\n'
    
    partial_log1 = '2024-04-17T10:53:17.355087080+00:00 stdout P 1st message\n'
    partial_log2_part1 = '2024-04-17T10:53:17.355087080+00:00 std'
    partial_log2_part2 = 'out F 3rd message\n'
    full_log_5 = '2024-04-17T10:53:17.355087080+00:00 stdout F Hello, TCP Server!\n'
    
    socket.sendall(metadata1.encode('utf-8'))
    socket.sendall(partial_log1.encode('utf-8'))
    socket.sendall(partial_log2_part1.encode('utf-8'))
    
    metadata2 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_d920a7cc-8a82-40e3-aef5-e40c2d6d1dd9_TammySweeney_1**\n'
    
    partial_log3 = '2024-04-17T10:53:17.355087080+00:00 stdout P 2nd message\n'
    partial_log4_part1 = '2024-04-17T10:53:17.355087080+00:00 stdout F 4t'
    partial_log4_part2 = 'h message\n'
    full_log_6 = '2024-04-17T10:53:17.355087080+00:00 stdout F Hello, TCP Server!\n'
    
    socket.sendall(metadata2.encode('utf-8'))
    socket.sendall(partial_log3.encode('utf-8'))
    socket.sendall(partial_log4_part1.encode('utf-8'))

    socket.sendall(metadata1.encode('utf-8'))
    socket.sendall(partial_log2_part2.encode('utf-8'))
    socket.sendall(full_log_6.encode('utf-8'))
    
    socket.sendall(metadata2.encode('utf-8'))
    socket.sendall(partial_log4_part2.encode('utf-8'))
    socket.sendall(full_log_5.encode('utf-8'))
        
    # print("Closing connection")

def diff_sockets(server_host, server_port):
    metadata2 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_d920a7cc-8a82-40e3-aef5-e40c2d6d1dd9_TammySweeney_1**\n'

    for i in range(500):
        now = datetime.datetime.now(datetime.timezone.utc)
        date_format = now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        log = f'{date_format} stdout P {i}\n'
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((server_host, server_port))
            sock.sendall(metadata2.encode('utf-8'))
            sock.sendall(log.encode('utf-8'))
            sock.close()
            print(f'Sent log: {log}')
        time.sleep(0.1)

def rotate_sockets(server_host, server_port):
    metadata2 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_d920a7cc-8a82-40e3-aef5-e40c2d6d1dd9_TammySweeney_1**\n'
    
    sockets = []
    # Generate 5 sockets
    for i in range(5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_host, server_port))
        sockets.append(sock)
        sock.sendall(metadata2.encode('utf-8'))
        
    for i in range(500):
        now = datetime.datetime.now(datetime.timezone.utc)
        date_format = now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        log = f'{date_format} stdout P {i}\n'
        sockets[i % 5].sendall(log.encode('utf-8'))
        print(f'Sent log: {log}')
        time.sleep(0.1)

    for sock in sockets:
        sock.close()


def rotate_sockets_with_metadata_switch(server_host, server_port):
    metadata1 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_60ddad09-9af4-4539-b556-cc3306b153e6_TammySweeney_2**\n'
    metadata2 = '\n**AlazLogs_ddosify_prometheus-deployment-5d7cdbb59-st4n5_3a41aa11-ffb3-4794-893b-8e137e5bfe27_TammySweeney_1**\n'
    
    sockets = []
    # Generate 5 sockets
    for i in range(5):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((server_host, server_port))
        sockets.append(sock)
        
    try:
        for i in range(5000):
            now = datetime.datetime.now(datetime.timezone.utc)
            date_format = now.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            log = f'{date_format} stdout P {i}\n'
            sockets[i % 5].sendall(metadata1.encode('utf-8'))
            sockets[i % 5].sendall(log.encode('utf-8'))
            sockets[i % 5].sendall(metadata2.encode('utf-8'))
            sockets[i % 5].sendall(log.encode('utf-8'))
            print(f'Sent logs for both metadata: {log}')
            time.sleep(20)
    except Exception as e:
        print(f'Error: {e}')
    finally:
        print("Closing connections")
        for sock in sockets:
            sock.close()

def connect(server_host, server_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_host, server_port))
    sock.close()

def simply_send_logs(server_host, server_port):
    metadata = '**AlazLogs_monitoring_prometheus-deployment-5d7cdbb59-st4n5_1899f451-e66b-45f2-88c2-22519ffe70d6_TammySweeney_1**\n'
    log = '2024-06-06T07:44:23.129370409Z stdout F { "msec": "1717659863.128","connection": "6911805","connection_requests": "1","pid": "29","request_id": "513811f0bbdaa17bbd960e701d89ee54","request_length": "0","remote_addr": "78.153.140.177","remote_user": "","remote_port": "58596","time_local": "06/Jun/2024:07:44:23 +0000","time_iso8601": "2024-06-06T07:44:23+00:00","request": "\\u0016\\u0003\\u0001\\u0001H\\u0001\\u0000\\u0001D\\u0003\\u0003\x98F\xdcf\xf5\xe0x\\u0010\xbd\x85027\x8f\x9b\\u001E\xe2\x90\xc28\\u000B\x8f#\xcc>+k\xd5","request_uri": "","args": "","status": "400","body_bytes_sent": "150","bytes_sent": "280","http_referer": "","http_user_agent": "","http_x_forwarded_for": "","http_host": "","server_name": "_","request_time": "0.073","upstream": "","upstream_connect_time": "","upstream_header_time": "","upstream_response_time": "","upstream_response_length": "","upstream_cache_status": "","ssl_protocol": "","ssl_cipher": "","scheme": "http","request_method"'
    
    log2 = '2024-06-06T10:09:34.716278454Z stdout F { "msec": "1717668574.715","connection": "7053690","connection_requests": "1","pid": "29","request_id": "1b94a3a4662c39a47b473e91b9d717d7","request_length": "0","remote_addr": "66.240.205.34","remote_user": "","remote_port": "58388","time_local": "06/Jun/2024:10:09:34 +0000","time_iso8601": "2024-06-06T10:09:34+00:00","request": "H\\u0000\\u0000\\u0000tj\xa8\x9e#D\x98+\xca\xf0\xa7\xbbl\xc5\\u0019\xd7\x8d\xb6\\u0018\xedJ\\u001En\xc1\xf9xu[l\xf0E\\u001D-j\xec\xd4xL\xc9r\xc9\\u0015\\u0010u\xe0%\x86Rtg\\u0005fv\x86]%\xcc\x80\\f\xe8\xcf\xae\\u0000\xb5\xc0f\xc8\x8dD\xc5\\t\xf4","request_uri": "","args": "","status": "400","body_bytes_sent": "150","bytes_sent": "280","http_referer": "","http_user_agent": "","http_x_forwarded_for": "","http_host": "","server_name": "_","request_time": "0.151","upstream": "","upstream_connect_time": "","upstream_header_time": "","upstream_response_time": "","upstream_response_length": "","upstre'

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((server_host, server_port))
        sock.sendall(metadata.encode('utf-8'))
        # sock.sendall(log.encode('utf-8'))
        sock.sendall(log2.encode('utf-8'))

        print("Closing connection")

def tcp_client(server_host, server_port, message):
    # connect(server_host, server_port)

    simply_send_logs(server_host, server_port)
    
    # diff_sockets(server_host, server_port)
    # rotate_sockets(server_host, server_port)
    # rotate_sockets_with_metadata_switch(server_host, server_port)
    
    # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect to server
        # sock.connect((server_host, server_port))
        # print(f'Connected to {server_host}:{server_port}')
        
        # no_split(sock)
        # split(sock)


        # Send data
        # for i in range(5):
        #     # metadata = '**AlazLogs_monitoring_prometheus-deployment-5d7cdbb59-st4n5_1899f451-e66b-45f2-88c2-22519ffe70d6_TammySweeney_1**\n'
        #     metadata = '**AlazLogs_kube-system-mock_prometheus-deployment-5d7cdbb59-st4n5_b824b98c-1844-470c-9e47-ac14056be80b_TammySweeney_1**\n'
        #     # partial_log1 = '2024-04-17T10:53:17.355087080+00:00 stdout P First message\n'
        #     partial_log1 = f'2024-05-28T10:53:17.355087080Z stdout P First message {i}\n'
        #     partial_log2 = f'2024-05-28T10:53:17.355087080+00:00 stdout F Final message {i}\n'
        #     # partial_log2 = f'2024-04-17T10:53:17.355087080+00:00 stdout F {"a": 1, "b": "cesd", "c": {"d": 1}}\n'
        #     # full_log = '2024-04-17T10:53:17.355087080+00:00 stdout F Hello, TCP Server!\n'
        #     sock.sendall(metadata.encode('utf-8'))
        #     sock.sendall(partial_log1.encode('utf-8'))
        #     sock.sendall(partial_log2.encode('utf-8'))

        #     # time.sleep(1)

        # print("Closing connection")


if __name__ == '__main__':
    HOST, PORT = '127.0.0.1', 9999  # Server address and port
    message = 'Hello, TCP Server!'   # Message to send
    # message = 'AlazLogs_ns_podName_podUid_containerName_1'
    tcp_client(HOST, PORT, message)
