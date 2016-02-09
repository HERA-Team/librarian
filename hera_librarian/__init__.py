# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

import json, os, urllib


class NoSuchSiteError (Exception):
    def __init__ (self, site_name):
        super (NoSuchSiteError, self).__init__ ("no such site " + repr (site_name))


def get_client_config():
    """Parse the client configuration file and return it as a dictionary."""
    path = os.path.expanduser('~/.hl_client.cfg')
    with open(path, 'r') as f:
        s = f.read()
    return json.loads(s)


def get_site(site_name):
    config = get_client_config()
    site = config['sites'].get (site_name)
    if site is None:
        raise NoSuchSiteError (site_name)
    return site


def _do_http_post(req, site):
    """do a POST operation, passing a JSON version of the request and expecting a
    JSON reply; return the decoded version of the latter.

    """
    req_json = json.dumps(req)
    params = urllib.urlencode({'request': req_json})
    url = site['url']+'/hl_rpc_handler.php'
    f = urllib.urlopen(url , params);
    reply_json = f.read()
    try:
        reply = json.loads(reply_json)
    except ValueError:
        print('failed to parse reply as JSON: ' + reply_json)
        raise
    return reply


# RPC calls

def create_observation(site_name, obs_id, julian_date, polarization, length):
    site = get_site(site_name)

    req = {
        'operation': 'create_observation',
        'authenticator': site['authenticator'],
        'id': obs_id,
        'julian_date': julian_date,
        'polarization': polarization,
        'length': length,
    }
    return _do_http_post(req, site)


def create_file(site_name, store_name, file_name, type, obs_id, size, md5):
    site = get_site(site_name)

    req = {
        'operation': 'create_file',
        'authenticator': site['authenticator'],
        'store_name': store_name,
        'file_name': file_name,
        'type': type,
        'obs_id': obs_id,
        'size': size,
        'md5': md5,
    }
    return _do_http_post(req, site)


def delete_file(site_name, file_name, store_name):
    site = get_site(site_name)

    req = {
        'operation': 'delete_file',
        'authenticator': site['authenticator'],
        'name': file_name,
        'store_name': store_name,
    }
    return _do_http_post(req, site)


def get_store_list(site_name):
    site = get_site(site_name)

    req = {
        'operation': 'get_store_list',
        'authenticator': site['authenticator'],
    }
    return _do_http_post(req, site)


def recommended_store(site_name, file_size):
    site = get_site(site_name)

    req = {
        'operation': 'recommended_store',
        'authenticator': site['authenticator'],
        'file_size': file_size,
    }
    return _do_http_post(req, site)


def create_copy_task(task_type, local_site_name, local_store_name, file_name,
                     remote_site_name, remote_store_name, delete_when_done):
    site = get_site(local_site_name)

    req = {
        'operation': 'create_copy_task',
        'authenticator': site['authenticator'],
        'task_type': task_type,
        'local_store_name': local_store_name,
        'file_name': file_name,
        'remote_site_name': remote_site_name,
        'remote_store_name': remote_store_name,
        'delete_when_done': delete_when_done,
    }
    return _do_http_post(req, site)
