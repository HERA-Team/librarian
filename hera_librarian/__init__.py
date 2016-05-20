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


    def assign_observing_sessions(self):
        return self._do_http_post ('assign_observing_sessions')


    def upload_file(self, local_path, dest_store_path, meta_mode, rec_info={}):
        """Upload the file located at `local_path` to the Librarian. We suggest a
        destination "store path" (something like "2345678/mydata.uv"), but the
        Librarian has to tell us which store to actually put the file on.

        The Librarian needs to contextual metadata to organize the new file
        appropriately (obsid, etc). This can be obtain in several ways:

        * If `meta_mode` is "direct", the appropriate information is stored in
          the the `rec_info` dict. That dict's contents have been provided to
          us from a different (probably local) Librarian; see
          `librarian_server.misc:gather_records`.
        * If `meta_mode` is "infer", the destination Librarian will attempt to
          infer metadata from the file itself. It can only do this for certain
          kinds of files, and there are certain kinds of value-added data that
          cannot be inferred. This mode should therefore be avoided when
          possible.

        This function invokes an SCP that is potentially trying to copy
        gigabytes of data across oceans. It may take a looong time to return
        and will not infrequently raise an exception.

        """
        if os.path.isabs (dest_store_path):
            raise Exception ('destination path may not be absolute; got %r' % (dest_store_path,))

        # In the first stage, we tell the Librarian how much data we're going to upload,
        # send it the database records, and get told the staging directory.

        from . import utils
        kwargs = {'upload_size': utils.get_size_from_path (local_path)}
        kwargs.update (rec_info)
        info = self._do_http_post ('initiate_upload', **kwargs)

        from .store import Store
        store = Store (info['name'], info['path_prefix'], info['ssh_host'])
        staging_dir = info['staging_dir']

        # Now, (try to) actually copy the data. This runs an SCP, potentially
        # across the globe, that in the real world will occasionally stall or
        # die or whatever.

        staged_path = os.path.join (staging_dir, os.path.basename (local_path))
        store.copy_to_store (local_path, staged_path)

        # If we made it here, though, the upload succeeded and we can tell
        # that Librarian that the data are ready to go. It will verify the
        # upload and ingest it.

        return self._do_http_post ('complete_upload',
            store_name=store.name,
            staging_dir=staging_dir,
            dest_store_path=dest_store_path,
            meta_mode=meta_mode,
        )


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
