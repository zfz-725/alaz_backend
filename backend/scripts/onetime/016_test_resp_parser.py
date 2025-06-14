import json
import os
import sys
import django

sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()

from core.utils import parse_resp

        
expected_results = {
    # '+OK\r\n': 'OK',
    # '-ERR unknown command "foobar"\r\n': 'ERR unknown command "foobar"',
    # '-Error message\r\n': 'Error message',
    # ':123\r\n': 123,
    # ':+123\r\n': 123,
    # ':-123\r\n': -123,
    # '$6\r\nfoobar\r\n': 'foobar',
    # '$0\r\n\r\n': '',
    # '$-1\r\n': None,
    # '*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n': ['foo', 'bar'],
    # '*0\r\n': [],
    # '*-1\r\n': None,
    # '*3\r\n:1\r\n:2\r\n:3\r\n': [1, 2, 3],
    # '*3\r\n:1\r\n:2\r\n$3\r\nfoo\r\n': [1, 2, 'foo'],
    # '*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n': [[1, 2, 3], ['Foo', 'Bar']],
    # '*4\r\n$5\r\nhello\r\n$5\r\nworld\r\n$-1\r\n$5\r\nagain\r\n': ['hello', 'world', None, 'again'],
    # '_\r\n': None,
    # '#t\r\n': True,
    # '#f\r\n': False,
    # ',1.23\r\n': 1.23,
    # ',-1.23\r\n': -1.23,
    # ',0\r\n': 0.0,
    # ',nan\r\n': float('nan'),
    # ',10\r\n': 10.0,
    # ',inf\r\n': float('inf'),
    # ',+inf\r\n': float('inf'),
    # ',-inf\r\n': float('-inf'),
    # '(1234567890123456789012345678901234567890\r\n': 1234567890123456789012345678901234567890,
    # '(0\r\n': 0,
    # '(-1234567890123456789012345678901234567890\r\n': -1234567890123456789012345678901234567890,
    # '!6\r\nfoobar\r\n': 'foobar',
    # '!21\r\nSYNTAX invalid syntax\r\n': 'SYNTAX invalid syntax',
    # '=15\r\ntxt:Some string\r\n': 'txt:Some string',
    # '%2\r\n$3\r\nfoo\r\n:1\r\n$3\r\nbar\r\n:2\r\n': {'foo':1, 'bar':2},
    # '%0\r\n': {},
    # '%-1\r\n': None,
    # '~3\r\n:1\r\n:2\r\n:3\r\n': {1, 2, 3},
    # '~0\r\n': set(),
    # '~-1\r\n': None,
    # '>3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n': ['foo', 'bar', 'baz'],
    # '>0\r\n': [],
    # '>-1\r\n': None,
    # '*24\r\n$5\r\nRPUSH\r\n$7\r\ntraffic\r\n$198\r\n{"cluster_id": "73d69f83-6f11-4c06-87f3-b55670438f95", "timestamp": 1717950234745, "tcp_seq_num": 1149801290, "thread_id": 1380096, "ingress": false, "node_id": "aks-hammerpool-13063871-vmss000002"}\r\n$198\r\n{"cluster_id": "73d69f83-6f11-4c06-87f3-b55670438f95", "timestamp": 1717950234745, "tcp_seq_num": 1149801290, "thread_id": 1380096, "ingress": false, "node_id": "aks-hammerpool-13063871-vmss000002"}\r\n$198\r\n{"cluster_id": "73d69f83-6f11-4c06-87f3-b55670438f95", "timestamp": 1717950234745, "tcp_seq_num": 1149801335, "thread_id": 1380096, "ingress": false, "node_id": "aks-hammerpool-13063871-vmss000002"}\r\n$198\r\n{"cluster_id": "73d69f83-6f11-4c06-87f3-b55670438f95", "timestamp": 1717950234745, "tcp_seq_num": 1149801335, "thread_id": 1380096, "ingress": false, "node_id": "aks-hammerpool-13063871-vmss000002"}\r\n$193\r\n{"cluster_id": "73d69f83-6f11-4c06-87f3-b55670438f95", "timestamp": 1717950234835, "tcp_seq_num": 288129921, "thread_id": 7043, "ingress": true, "node_id": "aks-hamm': None,
    '*41\r\n$5\r\nRPUSH\r\n$7\r\ntraffic\r\n$194\r\n{"cluster_id": "08d93256-2fbb-411e-b600-2a26e4f86227", "timestamp": 1720464737707, "tcp_seq_num": 3701739365, "thread_id": 18524, "ingress": false, "node_id": "aks-userpool-36567873-vmss00000o"}\r\n$194\r\n{"cluster_id": "08d93256-2fbb-411e-b600-2a26e4f86227", "timestamp": 1720464737707, "tcp_seq_num": 3701739365, "thread_id": 18524, "ingress": false, "node_id": "aks-userpool-36567873-vmss00000o"}\r\n$194\r\n{"cluster_id": "08d93256-2fbb-411e-b600-2a26e4f86227", "timestamp": 1720464737707, "tcp_seq_num": 3701739410, "thread_id": 18524, "ingress": false, "node_id": "aks-userpool-36567873-vmss00000o"}\r\n$194\r\n{"cluster_id": "08d93256-2fbb-411e-b600-2a26e4f86227", "timestamp": 1720464737707, "tcp_seq_num": 3701739410, "thread_id": 18524, "ingress": false, "node_id": "aks-userpool-36567873-vmss00000o"}\r\n$192\r\n{"cluster_id": "08d93256-2fbb-411e-b600-2a26e4f86227", "timestamp": 1720464737754, "tcp_seq_num": 3896560269, "thread_id": 7678, "ingress": true, "node_id": "aks-userpool-36567873-v': None,
}

for example in expected_results:
    # try:
        # response = parse_resp(example)
    # except Exception as e:
        # print(f'Error parsing {example}: {e}\n')
        # continue

    response = parse_resp(example)

    # if response[0] != expected_results[example]:
    #     print(f'Error parsing {example}: expected {expected_results[example]}, got {response[0]}\n')
    
    print(f'{example.encode()} -----> {response}\n')