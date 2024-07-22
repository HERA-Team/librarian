"""
A transfer manager for Globus transfers.
"""

import os
from pathlib import Path
from typing import Union

import globus_sdk
from pydantic import ConfigDict

from hera_librarian.transfer import TransferStatus

from .core import CoreAsyncTransferManager


class GlobusAsyncTransferManager(CoreAsyncTransferManager):
    """
    A transfer manager that uses Globus. This requires the
    local endpoint, the destiation endpoint, and the secret
    for authentication.
    """

    # We need the following to save the `authorizer` attribute without having
    # to build our own pydantic model for Globus-provided classes.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    destination_endpoint: str
    # The Globus endpoint UUID for the destination, entered in the configuration.

    native_app: bool = False
    # Whether to use a Native App (true) or a Confidential App (false, default)
    # for authorizing the client.

    transfer_attempted: bool = False
    transfer_complete: bool = False
    task_id: str = ""

    authorizer: Union[
        globus_sdk.RefreshTokenAuthorizer,
        globus_sdk.AccessTokenAuthorizer,
        None,
    ] = None
    # Default to `None`, but allow us to save Authorizer objects on the object

    def authorize(self, settings: "ServerSettings"):
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
        if settings.globus_enable is False:
            return False

        if self.authorizer is None:
            if settings.globus_client_native_app:
                try:
                    client = globus_sdk.NativeAppAuthClient(settings.globus_client_id)
                    self.authorizer = globus_sdk.RefreshTokenAuthorizer(
                        settings.globus_client_secret, client
                    )
                except globus_sdk.AuthAPIError as e:
                    return False
            else:
                try:
                    client = globus_sdk.ConfidentialAppAuthClient(
                        settings.globus_client_id, settings.globus_client_secret
                    )
                    tokens = client.oauth2_client_credentials_tokens()
                    transfer_tokens_info = tokens.by_resource_server[
                        "transfer.api.globus.org"
                    ]
                    transfer_token = transfer_tokens_info["access_token"]
                    self.authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
                except globus_sdk.AuthAPIError:
                    return False

        return True

    def valid(self, settings: "ServerSettings") -> bool:
        """
        Test whether it's valid to use Globus or not.

        Technically this only checks that we can authenticate with Globus and
        does not verify that we can copy files between specific endpoints.
        However, this is an important starting point and can fail for reasons of
        network connectivity, Globus as a service being down, etc.
        """
        return self.authorize(settings=settings)

    def _get_transfer_data(self, label: str, settings: "ServerSettings"):
        """
        This is a helper function to create a TransferData object, which is needed
        both for single-book transfers and batch transfers.
        """
        # create a TransferData object that contains options for the transfer
        transfer_data = globus_sdk.TransferData(
            source_endpoint=settings.globus_local_endpoint_id,
            destination_endpoint=self.destination_endpoint,
            label=label,
            sync_level="exists",
            verify_checksum=True,  # We do this ourselves, but globus will auto-retry if it detects failed files
            preserve_timestamp=True,
            notify_on_succeeded=False,
        )

        return transfer_data

    def transfer(
        self,
        local_path: Path,
        remote_path: Path,
        settings: "ServerSettings",
    ) -> bool:
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
        """
        self.transfer_attempted = True

        # start by authorizing
        if not self.authorize(settings=settings):
            return False

        # create a label from the name of the book
        label = os.path.basename(local_path)

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)

        # get a TransferData object
        transfer_data = self._get_transfer_data(label=label, settings=settings)

        # We need to figure out if the local path is actually a directory or a
        # flat file, which annoyingly requires different handling as part of the
        # Globus transfer.
        transfer_data.add_item(
            str(local_path), str(remote_path), recursive=local_path.is_dir()
        )

        # try to submit the task
        try:
            task_doc = transfer_client.submit_transfer(transfer_data)
        except globus_sdk.TransferAPIError as e:
            return False

        self.task_id = task_doc["task_id"]
        return True

    def batch_transfer(
        self,
        paths: list[tuple[Path]],
        settings: "ServerSettings",
    ) -> bool:
        self.transfer_attempted = True

        # We have to do a lot of the same legwork as above for a single
        # transfer, with the biggest change being that we can add multiple items
        # to a single TransferData object. This is effectively how we "batch"
        # books using Globus.

        # start by authorizing
        if not self.authorize(settings=settings):
            return False

        # make a label from the first book
        label = "batch with " + os.path.basename(paths[0][0])

        # create a transfer client to handle the transfer
        transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)

        # get a TransferData object
        transfer_data = self._get_transfer_data(label=label, settings=settings)

        # add each of our books to our task
        for local_path, remote_path in paths:
            # We need to figure out if the local path is actually a directory or a
            # flat file, which annoyingly requires different handling as part of the
            # Globus transfer.
            transfer_data.add_item(
                str(local_path), str(remote_path), recursive=local_path.is_dir()
            )

        # submit the transfer
        try:
            task_doc = transfer_client.submit_transfer(transfer_data)
        except globus_sdk.TransferAPIError as e:
            return False

        self.task_id = task_doc["task_id"]
        return True

    def transfer_status(self, settings: "ServerSettings") -> TransferStatus:
        """
        Query Globus to see if our transfer has finished yet.
        """
        if not self.authorize(settings=settings):
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
            transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)
            task_doc = transfer_client.get_task(self.task_id)

            if task_doc["status"] == "SUCCEEDED":
                return TransferStatus.COMPLETED
            elif task_doc["status"] == "FAILED":
                return TransferStatus.FAILED
            else:  # "status" == "ACTIVE"
                return TransferStatus.INITIATED
