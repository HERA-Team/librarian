# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function

import json, os.path, urllib

__all__ = str ('''
NoSuchSiteError
RPCError
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


class RPCError (Exception):
    def __init__ (self, req, message):
        super (RPCError, self).__init__ ("RPC call %r failed: %s" % (req, message))
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

        A minimal `site_config` dict should contain keys "authenticator" and
        "url", which are used to contact the Librarian's RPC API.

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
            raise RPCError (kwargs, 'failed to parse reply as JSON: ' + repr(reply))

        if not reply_json.get ('success', False):
            raise RPCError (kwargs,
                            reply_json.get ('message', '<no error message provided>'))

        return reply_json


    def delete_file(self, file_name, store_name):
        return self._do_http_post ('delete_file',
            name=file_name,
            store_name=store_name,
        )


    def create_file_event(self, file_name, type, **kwargs):
        """Note that keyword arguments to this function will automagically be stuffed
        inside the "payload" parameter.

        """
        return self._do_http_post ('create_file_event',
            file_name=file_name,
            type=type,
            payload=kwargs,
        )


    def get_recommended_store(self, file_size):
        from .store import Store
        info = self._do_http_post ('recommended_store', file_size=file_size)
        return Store (info['name'], info['path_prefix'], info['ssh_host'])


    def complete_upload(self, store_name, size, md5, type, obsid, start_jd, dest_store_path, create_time=None):
        return self._do_http_post ('complete_upload',
            store_name=store_name,
            size=size,
            md5=md5,
            type=type,
            obsid=obsid,
            start_jd=start_jd,
            dest_store_path=dest_store_path,
            create_time_unix=create_time,
        )


    def assign_observing_sessions(self):
        return self._do_http_post ('assign_observing_sessions')


    def upload_file (self, local_path, dest_store_path, type=None, start_jd=None, obsid=None, create_time=None):
        from . import utils
        size = utils.get_size_from_path (local_path)
        md5 = utils.get_md5_from_path (local_path)

        # We can infer essential metadata from some kinds of files, but not all.

        if type is None:
            type = utils.get_type_from_path (local_path)

        if type is None:
            raise Exception ('need to, but cannot, infer type of %r' % local_path)

        if obsid is None:
            obsid = utils.get_obsid_from_path (local_path)

        if obsid is None:
            raise Exception ('need to, but cannot, infer obsid of %r' % local_path)

        if start_jd is None:
            start_jd = utils.get_start_jd_from_path (local_path)

        if start_jd is None:
            raise Exception ('need to, but cannot, infer start_jd of %r' % local_path)

        # OK to go.

        store = self.get_recommended_store (size)
        store.stage_file_on_store (local_path)
        self.complete_upload (store.name, size, md5, type, obsid, start_jd, dest_store_path, create_time=create_time)


    def register_instance(self, store_name, store_path, type=None, obsid=None, start_jd=None, create_time=None):
        return self._do_http_post ('register_instance',
            store_name=store_name,
            store_path=store_path,
            type=type,
            obsid=obsid,
            start_jd=start_jd,
            create_time_unix=create_time,
        )


    def launch_file_copy(self, file_name, connection_name, remote_store_path=None):
        return self._do_http_post ('launch_file_copy',
            file_name=file_name,
            connection_name=connection_name,
            remote_store_path=remote_store_path,
        )


    def describe_session_without_event (self, source, event_type):
        return self._do_http_post ('describe_session_without_event',
            source=source,
            event_type=event_type,
        )
