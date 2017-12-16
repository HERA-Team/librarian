# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.

from __future__ import absolute_import, division, print_function

import json
import os.path
import urllib

__all__ = str('''
NoSuchConnectionError
RPCError
LibrarianClient
''').split()


class NoSuchConnectionError (Exception):
    def __init__(self, conn_name):
        super(NoSuchConnectionError, self).__init__("no such connection " + repr(conn_name))
        self.conn_name = conn_name


def get_client_config():
    """Parse the client configuration file and return it as a dictionary."""
    path = os.path.expanduser('~/.hl_client.cfg')
    with open(path, 'r') as f:
        s = f.read()
    return json.loads(s)


class RPCError (Exception):
    def __init__(self, req, message):
        super(RPCError, self).__init__("RPC call %r failed: %s" % (req, message))
        self.req = req
        self.message = message


def _normalize_deletion_policy(deletion_policy):
    # Keep this test in sync with librarian_server/file.py:DeletionPolicy.parse_safe()
    if deletion_policy not in ('allowed', 'disallowed'):
        raise Exception('unrecognized deletion policy %r' % (deletion_policy,))
    return deletion_policy


class LibrarianClient (object):
    conn_name = None
    "The name of the Librarian connection we target."

    config = None
    "The JSON config fragment corresponding to the desired connection."

    def __init__(self, conn_name, conn_config=None):
        """If `conn_config` is not None, it should be a dict containing at least the
        entries "authenticator" and "url" that define how to talk to the
        target Librarian. Otherwise, the file `~/.hl_client.cfg` will be used
        to look up a dict containing the same information.

        A minimal `conn_config` dict should contain keys "authenticator" and
        "url", which are used to contact the Librarian's RPC API.

        """
        self.conn_name = conn_name

        if conn_config is not None:
            self.config = conn_config
        else:
            config = get_client_config()
            self.config = config['connections'].get(conn_name)
            if self.config is None:
                raise NoSuchConnectionError(conn_name)

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
        f = urllib.urlopen(url, params)
        reply = f.read()
        try:
            reply_json = json.loads(reply)
        except ValueError:
            raise RPCError(kwargs, 'failed to parse reply as JSON: ' + repr(reply))

        if not reply_json.get('success', False):
            raise RPCError(kwargs,
                           reply_json.get('message', '<no error message provided>'))

        return reply_json

    def ping(self, **kwargs):
        return self._do_http_post('ping', **kwargs)

    def create_file_event(self, file_name, type, **kwargs):
        """Note that keyword arguments to this function will automagically be stuffed
        inside the "payload" parameter.

        """
        return self._do_http_post('create_file_event',
                                  file_name=file_name,
                                  type=type,
                                  payload=kwargs,
                                  )

    def assign_observing_sessions(self, minimum_start_jd=None, maximum_start_jd=None):
        return self._do_http_post('assign_observing_sessions',
                                  minimum_start_jd=minimum_start_jd,
                                  maximum_start_jd=maximum_start_jd,
                                  )

    def upload_file(self, local_path, dest_store_path, meta_mode, rec_info={},
                    deletion_policy='disallowed', known_staging_store=None,
                    known_staging_subdir=None, null_obsid=False):
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

        The caller can also specify whether the file instance that it creates is
        allowed to be deleted:

        * If `deletion_policy` is "disallowed", the default, it is not.
        * If `deletion_policy` is "allowed", it is.

        (In the future we might add more options.)

        If `meta_mode` is "infer' and `null_obsid` is True, the new file is
        expected and required to have a null `obsid` field, and it will be
        ingested into the Librarian as such. This mode is used for maintenance
        files that are not associated with a particular observation or
        observing session. It is an error to set `null_obsid` to True for
        other values of `meta_mode`.

        This function invokes an rsync that is potentially trying to copy
        gigabytes of data across oceans. It may take a looong time to return
        and will not infrequently raise an exception.

        The rsync lands in a "staging directory" on one of the destination
        hosts, before it is moved to its final destination. If
        `known_staging_store` and `known_staging_subdir` are specified, the
        destination Librarian will use these values instead of creating a
        temporary directory on whichever of its stores has the most free
        space. This can be used to "ingest" data that were previously copied
        over using some scheme unknown to the Librarian.

        """
        if os.path.isabs(dest_store_path):
            raise Exception('destination path may not be absolute; got %r' % (dest_store_path,))

        deletion_policy = _normalize_deletion_policy(deletion_policy)

        if null_obsid and meta_mode != 'infer':
            raise Exception('null_obsid may only be True when meta_mode is "infer"')

        # In the first stage, we tell the Librarian how much data we're going to upload,
        # send it the database records, and get told the staging directory.

        from . import utils
        kwargs = {
            'upload_size': utils.get_size_from_path(local_path),
            'known_staging_store': known_staging_store,
            'known_staging_subdir': known_staging_subdir,
        }
        kwargs.update(rec_info)
        info = self._do_http_post('initiate_upload', **kwargs)

        from .store import Store
        store = Store(info['name'], info['path_prefix'], info['ssh_host'])
        staging_dir = info['staging_dir']

        # Now, (try to) actually copy the data. This runs an SCP, potentially
        # across the globe, that in the real world will occasionally stall or
        # die or whatever.

        staged_path = os.path.join(staging_dir, os.path.basename(dest_store_path))
        store.copy_to_store(local_path, staged_path)

        # If we made it here, though, the upload succeeded and we can tell
        # that Librarian that the data are ready to go. It will verify the
        # upload and ingest it. This call is when the server's FileInstance
        # record is created, so it's where the deletion_policy option comes
        # into play.

        return self._do_http_post('complete_upload',
                                  store_name=store.name,
                                  staging_dir=staging_dir,
                                  dest_store_path=dest_store_path,
                                  meta_mode=meta_mode,
                                  deletion_policy=deletion_policy,
                                  staging_was_known=(known_staging_store is not None),
                                  null_obsid=null_obsid,
                                  )

    def register_instances(self, store_name, file_info):
        return self._do_http_post('register_instances',
                                  store_name=store_name,
                                  file_info=file_info,
                                  )

    def locate_file_instance(self, file_name):
        return self._do_http_post('locate_file_instance',
                                  file_name=file_name,
                                  )

    def set_one_file_deletion_policy(self, file_name, deletion_policy, restrict_to_store=None):
        deletion_policy = _normalize_deletion_policy(deletion_policy)

        return self._do_http_post('set_one_file_deletion_policy',
                                  file_name=file_name,
                                  deletion_policy=deletion_policy,
                                  restrict_to_store=restrict_to_store,
                                  )

    def delete_file_instances(self, file_name, mode='standard', restrict_to_store=None):
        return self._do_http_post('delete_file_instances',
                                  file_name=file_name,
                                  mode=mode,
                                  restrict_to_store=restrict_to_store,
                                  )

    def delete_file_instances_matching_query(self, query, mode='standard', restrict_to_store=None):
        return self._do_http_post('delete_file_instances_matching_query',
                                  query=query,
                                  mode=mode,
                                  restrict_to_store=restrict_to_store,
                                  )

    def launch_file_copy(self, file_name, connection_name, remote_store_path=None,
                         known_staging_store=None, known_staging_subdir=None):
        return self._do_http_post('launch_file_copy',
                                  file_name=file_name,
                                  connection_name=connection_name,
                                  remote_store_path=remote_store_path,
                                  known_staging_store=known_staging_store,
                                  known_staging_subdir=known_staging_subdir,
                                  )

    def initiate_offload(self, source_store_name, dest_store_name):
        return self._do_http_post('initiate_offload',
                                  source_store_name=source_store_name,
                                  dest_store_name=dest_store_name,
                                  )

    def describe_session_without_event(self, source, event_type):
        return self._do_http_post('describe_session_without_event',
                                  source=source,
                                  event_type=event_type,
                                  )

    def launch_local_disk_stage_operation(self, user, search, dest_dir):
        return self._do_http_post('search',
                                  stage_user=user,
                                  search=search,
                                  stage_dest=dest_dir,
                                  type='instances-stores',
                                  output_format='stage-the-files-json',
                                  )

    def search_sessions(self, search):
        return self._do_http_post('search',
                                  search=search,
                                  output_format='session-listing-json',
                                  )

    def search_files(self, search):
        return self._do_http_post('search',
                                  search=search,
                                  output_format='file-listing-json',
                                  )

    def search_instances(self, search):
        return self._do_http_post('search',
                                  search=search,
                                  output_format='instance-listing-json',
                                  )

    def search_observations(self, search):
        return self._do_http_post('search',
                                  search=search,
                                  output_format='obs-listing-json',
                                  )
