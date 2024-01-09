# -*- mode: python; coding: utf-8 -*-
# Copyright 2016 the HERA Team.
# Licensed under the BSD License.


import json
import os.path
from pathlib import Path
import urllib.request, urllib.parse, urllib.error
from pkg_resources import get_distribution, DistributionNotFound
import requests
from typing import Optional
from pydantic import BaseModel

__all__ = str('''
all_connections
get_client_config
NoSuchConnectionError
RPCError
LibrarianClient
''').split()


try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    # package is not installed
    pass


class NoSuchConnectionError(Exception):
    def __init__(self, conn_name):
        super(NoSuchConnectionError, self).__init__("no such connection " + repr(conn_name))
        self.conn_name = conn_name


def get_client_config():
    """Parse the client configuration file and return it as a dictionary."""
    path = os.path.expanduser('~/.hl_client.cfg')
    with open(path, 'r') as f:
        s = f.read()
    return json.loads(s)


def all_connections():
    """Generate a sequence of LibrarianClient objects for all connections in the
    configuration file.

    """
    config = get_client_config()

    for name, info in config.get('connections', {}).items():
        yield LibrarianClient(name, info)


class RPCError(Exception):
    def __init__(self, req, message):
        super(RPCError, self).__init__("RPC call %r failed: %s" % (req, message))
        self.req = req
        self.message = message


def _normalize_deletion_policy(deletion_policy):
    # Keep this test in sync with librarian_server/file.py:DeletionPolicy.parse_safe()
    if deletion_policy not in ('allowed', 'disallowed'):
        raise Exception('unrecognized deletion policy %r' % (deletion_policy,))
    return deletion_policy

class LibrarianHTTPError(Exception):
    def __init__(self, url, status_code, reason, suggested_remedy):
        super(LibrarianHTTPError, self).__init__(
            f"HTTP request to {url} failed with status code {status_code} and reason {reason}."
        )
        self.url = url
        self.status_code = status_code
        self.reason = reason
        self.suggested_remedy = suggested_remedy


class LibrarianClient(object):
    conn_name = None
    "The name of the Librarian connection we target."

    config = None
    "The JSON config fragment corresponding to the desired connection."

    def __init__(self, conn_name, conn_config=None):
        """
        A class for interacting with a Librarian server.

        If `conn_config` is not None, it should be a dict containing at least
        the entries "url", and then "authenticator" (if the server uses
        authenticator-based authentication) XOR "github_username" and
        "github_pat" (if the server uses GitHub-based authentication), which
        define how to talk to the target Librarian. Otherwise, the file
        `~/.hl_client.cfg` will be used to look up a dict containing the same
        information.

        A minimal `conn_config` dict should contain keys "url", and then either
        "authenticator" OR "github_username" and "github_pat", which are used to
        contact the Librarian's RPC API. Note that having both defined will
        raise an error, as a server will only do authenticator- or GitHub-based
        authentication. The user a priori should know which one the server they
        are trying to contact uses.

        Parameters
        ----------
        conn_name : str
            A string defining the name of the connection to use.
        conn_config : dict or None
            A dictionary containing details for how to interact with the target
            Librarian. If None, then we read from the user's ~/.hl_client.cfg
            file.
        """
        self.conn_name = conn_name

        if conn_config is not None:
            self.config = conn_config

            if "url" in self.config:
                if not "/" == self.config["url"][-1]:
                    self.config["url"] += "/"
        else:
            config = get_client_config()
            self.config = config['connections'].get(conn_name)
            if self.config is None:
                raise NoSuchConnectionError(conn_name)

    def _do_http_post(self, operation, **kwargs):
        """do a POST operation, passing a JSON version of the request and expecting a
        JSON reply; return the decoded version of the latter.

        """
        # TODO: This is an awful way to do this configuration...
        # figure out if we're using authenticator- or GitHub-based authentication
        if "authenticator" in self.config:
            if "github_username" in self.config or "github_pat" in self.config:
                raise ValueError(
                    "both 'authenticator' and one or both of {'github_username', "
                    "'github_pat'} were specified in the config file. This is "
                    "not supported, please only use one or the other depending "
                    "on which remote server you are attempting to access."
                )
            kwargs['authenticator'] = self.config['authenticator']
        else:
            kwargs["github_username"] = self.config["github_username"]
            kwargs["github_pat"] = self.config["github_pat"]

        for k in list(kwargs.keys()):
            if kwargs[k] is None:
                kwargs.pop(k)
        req_json = json.dumps(kwargs)

        params = urllib.parse.urlencode({'request': req_json}).encode("utf-8")
        url = self.config['url'] + 'api/' + operation
        try:
            f = urllib.request.urlopen(url, params)
            reply = f.read()
        except urllib.error.HTTPError as err:
            reply = err.read()
        try:
            reply_json = json.loads(reply)
        except ValueError:
            raise RPCError(kwargs, 'failed to parse reply as JSON: ' + repr(reply))

        if not reply_json.get('success', False):
            raise RPCError(kwargs,
                           reply_json.get('message', '<no error message provided>'))

        return reply_json
    
    def do_pydantic_http_post(self, endpoint: str, request_model: Optional[BaseModel] = None, response_model: Optional[BaseModel] = None):
        """
        Do a POST operation, passing a JSON version of the request and expecting a
        JSON reply; return the decoded version of the latter.

        Parameters
        ----------
        endpoint : str
            The endpoint to post to.
        request_model : pydantic.BaseModel, optional
            The request model to send. If None, we don't ask for anything.
        response_model : pydantic.BaseModel, optional
            The response model to expect. If None, we don't return anything.

        Returns
        -------
        response_model
            The decoded response model.
        """
        full_endpoint = self.config["url"] + "api/v2/" + endpoint

        # Do not do authentication yet.
        data = None if request_model is None else request_model.model_dump_json()

        r = requests.post(
            full_endpoint, data=data, headers={"Content-Type": "application/json"}
        )

        # Decode the response.
        if r.status_code not in [200, 201]:
            try:
                json = r.json()
            except requests.exceptions.JSONDecodeError:
                json = {}
            
            raise LibrarianHTTPError(
                full_endpoint,
                r.status_code,
                r.json().get("reason", "<no reason provided>"),
                r.json().get("suggested_remedy", "<no suggested remedy provided>"),
            )
        
        if response_model is not None:
            # Note that the pydantic model wants the full bytes content
            # not the deserialized r.json()
            return response_model.model_validate_json(r.content)
        else:
            return None

    def ping(self):
        """
        Ping the remote librarian to see if it exists.

        Returns
        -------

        PingResponse
            The response from the remote librarian.

        Raises
        ------

        LibrarianHTTPError
            If the remote librarian is unreachable.

        pydantic.ValidationError
            If the remote librarian returns an invalid response.
        """
        from .models.ping import PingRequest, PingResponse

        response: PingResponse = self.do_pydantic_http_post(
            endpoint="ping",
            request_model=PingRequest(),
            response_model=PingResponse,
        )

        return response

    def probe_stores(self, **kwargs):
        return self._do_http_post('probe_stores', **kwargs)

    def stores(self):
        """Generate a sequence of Stores that are attached to the remote Librarian."""

        from .base_store import BaseStore
        info = self.probe_stores()

        for item in info['stores']:
            yield BaseStore(item['name'], item['path_prefix'], item['ssh_host'])


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
    
    def upload_file(
        self,
        local_path: Path,
        dest_path: Path,
        deletion_policy: str='disallowed',
        null_obsid: bool=False,
    ) -> dict:
        
        deletion_policy = _normalize_deletion_policy(deletion_policy)

        if dest_path.is_absolute():
            raise Exception(f"Destination path may not be absolute; got {dest_path}")
        
        # Ask the librarian for a staging directory, and a list of transfer managers
        # to try.

        from .utils import get_size_from_path, get_md5_from_path
        from .models.uploads import UploadInitiationRequest, UploadInitiationResponse, UploadCompletionRequest

        response: UploadInitiationResponse = self.do_pydantic_http_post(
            endpoint="upload/stage",
            request_model=UploadInitiationRequest(
                upload_size=get_size_from_path(local_path),
                upload_checksum=get_md5_from_path(local_path),
                upload_name=dest_path.name,
                destination_location=dest_path,
                # TODO: Figure out a programatic way of getting this.
                uploader="TEST_USER",
            ),
            response_model=UploadInitiationResponse,
        )

        from .transfers import CoreTransferManager

        transfer_managers = response.transfer_providers

        # Now try all the transfer managers. If they're valid, we try to use them.
        # If they fail, we should probably catch the exception.
        # TODO: Catch the exception on failure.
        used_transfer_manager: Optional[CoreTransferManager] = None
        used_transfer_manager_name: Optional[str] = None

        # TODO: Should probably have some manual ordering here.
        for name, transfer_manager in transfer_managers.items():
            if transfer_manager.valid:
                transfer_manager.transfer(
                    local_path=local_path, remote_path=response.staging_location
                )

                # We used this.
                used_transfer_manager = transfer_manager
                used_transfer_manager_name = name
                
                break
            else:
                print(f"Warning: transfer manager {name} is not valid.")

        if used_transfer_manager is None:
            raise Exception("No valid transfer managers found.")

        # If we made it here, the file is successfully on the store!

        request = UploadCompletionRequest(
            store_name=response.store_name,
            staging_name=response.staging_name,
            staging_location=response.staging_location,
            upload_name=response.upload_name,
            destination_location=dest_path,
            transfer_provider_name=used_transfer_manager_name,
            transfer_provider=used_transfer_manager,
            meta_mode="infer",
            deletion_policy=deletion_policy,
            # TODO: Figure out what source name actually does.
            # INFO: Source name is the person/librarian that uploaded this.
            source_name="",
            null_obsid=null_obsid,
            # TODO: Figure out how to get this programattically.
            uploader="TEST_USER",
            transfer_id=response.transfer_id,
        )

        self.do_pydantic_http_post(
            endpoint="upload/commit",
            request_model=request,
        )

        return
  
    def register_instances(self, store_name, file_info):
        return self._do_http_post(
            'register_instances',
            store_name=store_name,
            file_info=file_info,
        )

    def locate_file_instance(self, file_name):
        return self._do_http_post(
            'locate_file_instance',
            file_name=file_name,
        )

    def set_one_file_deletion_policy(
            self, file_name, deletion_policy, restrict_to_store=None
    ):
        deletion_policy = _normalize_deletion_policy(deletion_policy)

        return self._do_http_post(
            'set_one_file_deletion_policy',
            file_name=file_name,
            deletion_policy=deletion_policy,
            restrict_to_store=restrict_to_store,
        )

    def delete_file_instances(self, file_name, mode='standard', restrict_to_store=None):
        return self._do_http_post(
            'delete_file_instances',
            file_name=file_name,
            mode=mode,
            restrict_to_store=restrict_to_store,
        )

    def delete_file_instances_matching_query(
            self, query, mode='standard', restrict_to_store=None
    ):
        return self._do_http_post(
            'delete_file_instances_matching_query',
            query=query,
            mode=mode,
            restrict_to_store=restrict_to_store,
        )

    def launch_file_copy(
        self,
        file_name,
        connection_name,
        remote_store_path=None,
        known_staging_store=None,
        known_staging_subdir=None,
    ):
        return self._do_http_post(
            'launch_file_copy',
            file_name=file_name,
            connection_name=connection_name,
            remote_store_path=remote_store_path,
            known_staging_store=known_staging_store,
            known_staging_subdir=known_staging_subdir,
        )

    def initiate_offload(self, source_store_name, dest_store_name):
        return self._do_http_post(
            'initiate_offload',
            source_store_name=source_store_name,
            dest_store_name=dest_store_name,
        )

    def describe_session_without_event(self, source, event_type):
        return self._do_http_post(
            'describe_session_without_event',
            source=source,
            event_type=event_type,
        )

    def launch_local_disk_stage_operation(self, user, search, dest_dir):
        return self._do_http_post(
            'search',
            stage_user=user,
            search=search,
            stage_dest=dest_dir,
            type='instances-stores',
            output_format='stage-the-files-json',
        )

    def search_sessions(self, search):
        return self._do_http_post(
            'search',
            search=search,
            output_format='session-listing-json',
        )

    def search_files(self, search):
        return self._do_http_post(
            'search',
            search=search,
            output_format='file-listing-json',
        )

    def search_instances(self, search):
        return self._do_http_post(
            'search',
            search=search,
            output_format='instance-listing-json',
        )

    def search_observations(self, search):
        return self._do_http_post(
            'search',
            search=search,
            output_format='obs-listing-json',
        )

    def gather_file_record(self, file_name):
        return self._do_http_post('gather_file_record', file_name=file_name)

    def create_file_record(self, rec_info):
        return self._do_http_post('create_file_record', **rec_info)# 
