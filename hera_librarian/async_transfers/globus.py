"""
A transfer manager for Globus transfers.
"""

import os
from pathlib import Path

import globus_sdk

from hera_librarian.transfer import TransferStatus

from ..queues import Queue
from .core import CoreAsyncTransferManager

class GlobusAsyncTransferManager(CoreAsyncTransferManager):
    """
    A transfer manager that uses Globus. This requires the
    local endpoint, the destiation endpoint, and the secret
    for authentication.
    """

    client_id: str
    secret: str
    native_app: bool

    transfer_attempted: bool = False
    transfer_complete: bool = False
    task_id: str = ""

    def __init__(self, client_id, secret, native_app=False):
        """
        Create an asynchronous transfer manager for interacting with Globus.

        See the `authorize` method for more details on how authorization is
        performed.

        Parameters
        ----------
        client_id : str
            The client UUID associated with the entity initiating a transfer.
            Note that this is NOT an endpoint ID, and instead is tied either to
            a "thick client" or a "service account" used for authentication.
        secret : str
            The secret associated with the client. This should be either a
            "refresh token" (for a thick client) or a "client secret" (for a
            service account).
        native_app : bool, optional
            Whether to use a Native App (true) or a Confidential App (false,
            default) for authorizing the client.
        """

    def authorize(self):
        """
        Attempt to authorize using the Globus service.

        This method will attempt to authenticate with Globus. There are two
        primary objects that can be used for this: the NativeAppAuthClient, and
        the ConfidentialAppAuthClient. The Native App is used for having a user
        authenticate as "themselves", and is tied to a "thick client". The
        Confidential App is used for having the client authenticate as "itself",
        and is a "service account" not explicitly tied to a specific Globus user
        account.

        Note that the "secret" used is different in the two cases: for the
        Native App, the secret is assumed to be a Refresh Token, which is a
        long-lived token that allows the user to authenticate and initiate
        transfers. For the Confidential App, the secret is the Client Secret
        generated when the app was created.

        Once the authenticator is created, the way they work downstream is
        effectively interchangeable. Note that Globus as a service will perform
        further checking to see if the user/app has permission to read and write
        to specific endpoints. We will do our best to handle this as it comes up
        to provide the user with nicer error messages, though we may not have
        caught all possible failure modes.
        """
        if not hasattr(self, authorizer):
            if self.native_app:
                try:
                    client = globus_sdk.NativeAppAuthClient(self.client_id)
                    self.authorizer = globus_sdk.RefreshTokenAuthorizer(
                        secret, client
                    )
                except globus_sdk.AuthAPIError as e:
                    return False
            else:
                try:
                    client = globus_sdk.ConfidentialAppAuthClient(self.client_id)
                    tokens = client.oauth2_client_credentials_tokens()
                    transfer_tokens_info = (
                        tokens.by_resource_server["transfer.api.globus.org"]
                    )
                    transfer_token = transfer_tokens_info["access_token"]
                    self.authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
                except globus_sdk.AuthAPIError as e:
                    return False

        return True

    @property
    def valid(self) -> bool:
        """
        This is easy to check if we're using a system with a Globus Connect
        Personal (GCP) endpoint, but harder to verify for environments with
        Globus Connect Server endpoints (e.g., NERSC). For now, we're lazy and
        assume we can always use Globus (though this is obviously NOT always
        true).
        """
        return True

    def _get_task_data(self, local_endpoint, remote_endpoint, label):
        """
        This is a helper function to create a TaskData object, which is needed
        both for single-book transfers and batch transfers.
        """
        # create a TransferData object that contains options for the transfer
        task_data = globus_sdk.TransferData(
            source_endpoint=local_endpoint,
            destination_endpoint=remote_endpoint,
            label=label,
            sync_level="exists",
            verify_checksum=False,  # we do this ourselves
            preserve_timestamp=True,
            notify_on_succeeded=False,
        )

        return task_data

    def transfer(
        self,
        local_path: Path,
        remote_path: Path,
        local_endpoint: str,
        remote_endpoint: str,
    ) -> str:
        """
        Attempt to transfer a book using Globus.

        This method will attempt to create a Globus transfer. If successful, we
        will have set the task ID of the transfer on the object, which can be
        used to query Globus as to its status. If unsuccessful, we will have
        gotten nothing but sadness.

        Parameters
        ----------
        local_path : Path
            The local path for the transfer relative to the root Globus
            directory, which is generally not the same as /.
        remote_path : Path
            The remote path for the transfer relative to the root Globus
            directory, which is generally not the same as /.
        local_endpoint : str
            The Globus endpoint UUID for the local librarian.
        remote_endpoint : str
            The Globus endpoint UUID for the remote librarian.
        """
        self.transfer_attempted = True

        # start by authorizing
        if not self.authorize():
            return False

        # create a label from the name of the book
        label = os.path.basename(local_path)

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)

        # get a TaskData object
        task_data = _get_task_data(local_endpoint, remote_endpoint, label)

        # We need to figure out if the local path is actually a directory or a
        # flat file, which annoyingly requires different handling as part of the
        # Globus transfer.
        if local_path.is_dir():
            task_data.add_item(local_path, remote_path, recursive=True)
        else:
            task_data.add_item(local_path, remote_path, recursive=False)

        # try to submit the task
        try:
            task_doc = transfer_client.submit_transfer(task_data)
        except globus_sdk.TransferAPIError as e:
            return False

        self.task_id = task_doc["task_id"]
        return True

    def batch_transfer(
        self,
        paths: list[tuple[Path]],
        local_endpoint,
        remote_endpoint,
    ):
        self.transfer_attempted = True

        # We have to do a lot of the same legwork as above for a single
        # transfer, with the biggest change being that we can add multiple items
        # to a single TaskData object. This is effectively how we "batch" books
        # using Globus.

        # start by authorizing
        if not self.authorize():
            return False

        # make a label from the first book
        label = "batch with " + os.path.basename(paths[0][0])

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)

        # get a TaskData object
        task_data = _get_task_data(local_endpoint, remote_endpoint, label)

        # add each of our books to our task
        for local_path, remote_path in paths:
            # We need to figure out if the local path is actually a directory or a
            # flat file, which annoyingly requires different handling as part of the
            # Globus transfer.
            if local_path.is_dir():
                task_data.add_item(local_path, remote_path, recursive=True)
            else:
                task_data.add_item(local_path, remote_path, recursive=True)

        # submit the transfer
        try:
            task_doc = transfer_client.submit_transfer(task_data)
        except globus_sdk.TransferAPIError as e:
            return False

        self.task_id = task_doc["task_id"]
        return True

    @property
    def transfer_status(self) -> TransferStatus:
        """
        Query Globus to see if our transfer has finished yet.
        """
        if not self.authorize():
            # We *should* be able to just assume that we have already
            # authenticated and should be able to query the status of our
            # transfer. However, if for whatever reason we're not able to talk
            # to Globus (network issues, Globus outage, etc.), we won't be able
            # to find out our transfer's status -- let's bail and assume we
            # failed
            return TransferStatus.FAILED

        if self.task_id == "":
            if not self.transfer_attempted:
                return TransferStatus.INITIATED
            else:
                return TransferStatus.FAILED
        else:
            # start talking to Globus
            transfer_client = globus_sdk.transfer_client(
                authorizer=self.authorizer
            )
            task_doc = transfer_client.get_task(self.task_id)

            if task_doc["status"] == "SUCCEEDED":
                return TransferStatus.COMPLETED
            elif task_doc["status"] == "FAILED":
                return TransferStatus.FAILED
            else:  # "status" == "ACTIVE"
                return TransferStatus.INITIATED
