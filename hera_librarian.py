import os
import urllib
import json

# parse config file, return as a dictionary
#
def get_config():
    path = os.path.expanduser('~/.hera_librarian')
    f = open(path, 'r')
    config = {}
    for line in f:
        wds = line.split()
        config[wds[0]] = wds[1]
    f.close()
    return config

# do a POST operation,
# passing a JSON version of the request and expecting a JSON reply;
# return the decoded version of the latter.
#
def do_http_post(req, config):
    req_json = json.dumps(req)
    params = urllib.urlencode({'request': req_json})
    url = config['server']+'/hl_rpc_handler.php'
    f = urllib.urlopen(url , params);
    reply_json = f.read()
    reply = json.loads(reply_json)
    return reply

# RPC to create a file
#
def create_file(name, size, md5, store_name):
    config = get_config()
    req = {'operation': 'create_file',
        'authenticator': config['authenticator'],
        'name': name,
        'size': size,
        'md5': md5,
        'store_name': store_name}
    return do_http_post(req, config)

#create_file('filename2', 2e9, 'ajfjfkdjffjf', 'UCB RAID')
