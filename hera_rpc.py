# utility functions for Python RPC bindings

import os
import urllib
import json

# parse config file, return as dictionary
#
def get_config(file):
    path = os.path.expanduser('~/'+file)
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
def do_http_post(req, server):
    req_json = json.dumps(req)
    params = urllib.urlencode({'request': req_json})
    url = server+'/hl_rpc_handler.php'
    f = urllib.urlopen(url , params);
    reply_json = f.read()
    reply = json.loads(reply_json)
    return reply

