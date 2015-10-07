import os
import urllib
import json

def get_config():
    path = os.path.expanduser('~/.hera_librarian')
    f = open(path, 'r')
    config = {}
    for line in f:
        wds = line.split()
        config[wds[0]] = wds[1]
    f.close()
    return config

def create_file(name, size, md5):
    config = get_config()
    req = {'operation': 'create_file',
        'authenticator': config['authenticator'],
        'name': name,
        'size': size,
        'md5': md5}

    req_json = json.dumps(req)
    params = urllib.urlencode({'request': req_json})
    url = config['server']+'/hl_rpc_handler.php'
    f = urllib.urlopen(url , params);

    reply_json = f.read()
    reply = json.loads(reply_json)
    return reply

create_file('filename2', 2e9, 'ajfjfkdjffjf')
