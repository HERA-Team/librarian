# utility functions for Python RPC bindings

import os
import urllib
import json

# parse client config file, return as dictionary
#
def get_client_config():
    path = os.path.expanduser('~/'+'.hl_client.cfg')
    with open(path, 'r') as f:
        s = f.read()
    return json.loads(s)

# do a POST operation,
# passing a JSON version of the request and expecting a JSON reply;
# return the decoded version of the latter.
#
def do_http_post(req, site):
    req_json = json.dumps(req)
    params = urllib.urlencode({'request': req_json})
    url = site['url']+'/hl_rpc_handler.php'
    f = urllib.urlopen(url , params);
    reply_json = f.read()
    reply = json.loads(reply_json)
    return reply

