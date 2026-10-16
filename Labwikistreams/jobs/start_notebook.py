import json
import uuid
from pathlib import Path

import requests
import websocket

server = json.loads(next(Path('/home/jovyan/.local/share/jupyter/runtime').glob('jpserver-*.json')).read_text())
token = server['token']
base = 'http://127.0.0.1:8888'
headers = {'Authorization': f'token {token}'}
response = requests.post(f'{base}/api/sessions', headers=headers, json={
    'path': 'work/wikistream_pyspark.ipynb',
    'name': 'wikistream_pyspark.ipynb',
    'type': 'notebook',
    'kernel': {'name': 'python3'},
}, timeout=20)
response.raise_for_status()
kernel = response.json()['kernel']['id']
ws = websocket.create_connection(f'ws://127.0.0.1:8888/api/kernels/{kernel}/channels?token={token}', timeout=240)
notebook = json.loads(Path('/home/jovyan/work/wikistream_pyspark.ipynb').read_text())
source = '\n\n'.join(''.join(cell['source']) for cell in notebook['cells'] if cell['cell_type'] == 'code')
message_id = uuid.uuid4().hex
ws.send(json.dumps({
    'header': {'msg_id': message_id, 'username': 'lab', 'session': uuid.uuid4().hex,
               'msg_type': 'execute_request', 'version': '5.3'},
    'parent_header': {}, 'metadata': {}, 'channel': 'shell',
    'content': {'code': source, 'silent': False, 'store_history': True,
                'user_expressions': {}, 'allow_stdin': False, 'stop_on_error': True},
}))
while True:
    message = json.loads(ws.recv())
    if message.get('parent_header', {}).get('msg_id') == message_id and message['header']['msg_type'] == 'execute_reply':
        if message['content']['status'] != 'ok':
            raise RuntimeError(message['content'])
        break
ws.close()
print('Notebook is running. Open work/wikistream_pyspark.ipynb in Jupyter.')
