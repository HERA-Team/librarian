# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

import json, os, urllib

__all__ = str ('''
NoSuchSiteError
RPCFailedError
LibrarianClient
''').split ()


class NoSuchSiteError (Exception):
    def __init__ (self, site_name):
        super (NoSuchSiteError, self).__init__ ("no such site " + repr (site_name))
        self.site_name = site_name


def get_client_config():
    """Parse the client configuration file and return it as a dictionary."""
    path = os.path.expanduser('~/.hl_client.cfg')
    with open(path, 'r') as f:
        s = f.read()
    return json.loads(s)


class RPCFailedError (Exception):
    def __init__ (self, req, message):
        super (RPCFailedError, self).__init__ ("RPC call %r failed: %s" % (req, message))
        self.req = req
        self.message = message


class LibrarianClient (object):
    site_name = None
    "The name of the Librarian site we target."

    config = None
    "The JSON config fragment corresponding to the desired site."

    def __init__ (self, site_name, site_config=None):
        """If `site_config` is not None, it should be a dict containing at least the
        entries "authenticator" and "url" that define how to talk to the
        target Librarian. Otherwise, the file `~/.hl_client.cfg` will be used
        to look up a dict containing the same information.

        """
        self.site_name = site_name

        if site_config is not None:
            self.config = site_config
        else:
            config = get_client_config ()
            self.config = config['sites'].get (site_name)
            if self.config is None:
                raise NoSuchSiteError (site_name)


    def _do_http_post(self, operation, **kwargs):
        """do a POST operation, passing a JSON version of the request and expecting a
        JSON reply; return the decoded version of the latter.

        """
        kwargs['authenticator'] = self.config['authenticator']
        for k in kwargs.keys():
            if kwargs[k] is None:
                kwargs.pop(k)
        req_json = json.dumps(kwargs)

        params = urllib.urlencode({'request': req_json})
        url = self.config['url'] + 'api/' + operation
        f = urllib.urlopen(url, params);
        reply = f.read()
        try:
            reply_json = json.loads(reply)
        except ValueError:
            raise RPCFailedError (kwargs, 'failed to parse reply as JSON: ' + repr(reply))

        if not reply_json.get ('success', False):
            raise RPCFailedError (kwargs,
                                  reply_json.get ('message', '<no error message provided>'))

        return reply_json


    def create_observation(self, obsid, start_time_jd,
                           stop_time_jd=None, lst_start_hr=None):
        return self._do_http_post('create_or_update_observation',
            obsid=obsid,
            start_time_jd=start_time_jd,
            stop_time_jd=stop_time_jd,
            lst_start_hr=lst_start_hr,
        )


    def create_file(self, name, type, obsid, size, md5, create_time=None):
        return self._do_http_post ('create_or_update_file',
            name=name,
            type=type,
            obsid=obsid,
            size=size,
            md5=md5,
            create_time=create_time,
        )


    def list_files_without_history_item (self, source, hist_type):
        return self._do_http_post ('list_files_without_history_item',
            source=source,
            hist_type=hist_type,
        )


    def delete_file(self, file_name, store_name):
        return self._do_http_post ('delete_file',
            name=file_name,
            store_name=store_name,
        )


    def create_history(self, store_name, file_name, type, payload):
        return self._do_http_post ('create_history',
            store_name=store_name,
            file_name=file_name,
            type=type,
            payload=payload,
        )


    def get_store_list(self):
        return self._do_http_post ('get_store_list')


    def recommended_store(self, file_size):
        return self._do_http_post ('recommended_store',
            file_size=file_size
        )


    def create_copy_task (self, task_type, local_store_name, file_name,
                          remote_site_name, remote_store_name, delete_when_done):
        return self._do_http_post ('create_copy_task',
            task_type=task_type,
            local_store_name=local_store_name,
            file_name=file_name,
            remote_site_name=remote_site_name,
            remote_store_name=remote_store_name,
            delete_when_done=delete_when_done,
        )
