# Globus Functionality in the Librarian

[Globus](https://www.globus.org/) is a service for transferring research data
between data processing environments. The Librarian has functionality for making
use of globus to copy instances of files between Librarian stores. Below we
provide a brief overview of the Globus infrastructure as it relates to
interoperating with the Librarian, as well as provide instructions for how to
install and configure Globus for use in the Librarian. For additional
information about Globus and the services it provides, please refer to their
online documentation.

Globus installations on machines are referred to as "endpoints". The service
makes a distinction between "Personal Endpoints", which can be made for personal
machines and workstations, and "Server Endpoints", which are typically
facility-wide installations at large computing centers (e.g., XSEDE sites,
NERSC, etc.). When using Globus to transfer data, at least one endopint must be
a Server (unless the user is a "Premium" user, in which case a transfer can be
initiated between two Personal endpoints). The current Globus functionality in
the Librarian has been built and tested assuming that the user is running a
Personal Endpoint (e.g., on site where data is acquired) and copying data to a
Server Endpoint. Functionality for transferring between two Server Endpoints
should be straightforward to add if desired.

In the documentation below, we assume that the Server Endpoint has already been
installed by the destination machine's system administrators and is part of the
Globus network. We focus primarily on how to set up Globus on stores in the
Librarian. To programmatically use Globus for automated transfers in the
Librarian, it may be necessary to create a Shared Endpoint on the Globus
Server. Please refer to the [relevant section below](#globus-shared-endpoints).

### Table of Contents

- [Installing Globus on Librarian
  Stores](#installing-globus-on-librarian-stores)
- [Adding Globus Information to Config
  Files](#adding-globus-information-to-config-files)
  - [Librarian Server Configuration](#librarian-server-configuration)
  - [Remote Librarian Configuration](#remote-librarian-configuration)
- [Using Globus from the Librarian Command-Line
  Interface](#using-globus-from-the-librarian-command-line-interface)
- [Globus Shared Endpoints](#globus-shared-endpoints)


## Installing Globus on Librarian Stores

In general, each store (on a different machine) known to the Librarian should
have its own installation of Globus running. This is due to the fact that
transfers between stores and remote Librarian instances are launched on a
per-store basis, which in principle require unique identifiers. However, if
there are multiple stores attached to the same machine, then multiple
installations may not be required. The procedure outlined below should thus be
performed for each machine (not necessarily store) in a Librarian installation.

For the most up-to-date documentation for installing Globus on a Linux
installation, please refer to the [online
documentation](https://docs.globus.org/how-to/globus-connect-personal-linux/#globus-connect-personal-cli). As
mentioned above, this process should be done for each machine known to the
Librarian.

After the Globus Connect Personal client has been installed for each store, the
client must be configured to expose the store directories to the transfer
service. Follow [these
instructions](https://docs.globus.org/faq/globus-connect-endpoints/#how_do_i_configure_accessible_directories_on_globus_connect_personal_for_linux)
for information on how to export specific directories. For instance, if we
wanted to export the `/data` directory on a store, the entry in the
`config-paths` folder might read:
```
/data,0,0
```
where the last `0` means that this path is read-only. (This option is
appropriate for on-site installations where the flow of data is uni-directional
to the remote Globus Server installation.) Once the desired directories have
been specified, the Globus client should be started. Because this process will
be running in the background, it is best to launch the following command inside
of a `screen` session or as part of a daemon. Run
```bash
$ ./globusconnectpersonal -start
```
To confirm that the Globus installation provides the information necessary for
the Librarian, run the following command inside of a python interpreter:
```python
>>> from globus_sdk import LocalGlobusConnectPersonal
>>> local_ep = LocalGlobusConnectPersonal()
>>> local_ep.endpoint_id
'b19b3b45-01ae-11e6-a71c-22000bf2d559'
```
If no output value is generated, please ensure that the file
`~/.globusonline/lta/client-id.txt` exists. If the home drive is shared among
multiple store machines (e.g., due to a netboot cluster configuration),
additional steps must be taken to ensure that the output of the `endpoint_id`
attribute is unique to each machine and correct for a given store.


## Adding Globus Information to Config Files

Once Globus Connect Personal clients have been installed and started on
individual machines, addtional entries must be specified in both the Librarian
server configuration (e.g., `server-config.json`) *and* the remote Librarian
instance list (`~/.hl_client.cfg`).

### Librarian Server Configuration

There are certain config entries that *must* be added for the Librarian to use
Globus. They are:

- `use_globus`: a boolean value (`true` or `false`) indicating whether the
  Librarian should use Globus or not. (If not, `rsync` will be used for
  transfers.)
- `globus_client_id`: the Client ID of the Globus account associated with
  invoking transfers. This can be be retrieved after logging into Globus using
  the `globus-cli` python package (which can be installed with
  `pip install globus-cli`):
  ```bash
  $ globus whoami
  username@globusid.org
  $ globus get-identities "username@globusid.org"
  224532bb-8a4b-4d32-8995-e1fb442be98e
  ```
- `globus_transfer_token`: the Globus "refresh token" that should be used for
  logging into the service. Follow the steps in [this SDK documentation
  page](https://globus-sdk-python.readthedocs.io/en/stable/tutorial/#advanced-2-refresh-tokens-never-login-again)
  for information on how to obtain a refresh token. This allows for
  re-authenticating with Globus without intervention from the user. (**NOTE!**
  As mentioned in the tutorial, the refresh token should be treated as a
  password and, accordingly, not be stored in a public place. This means not
  uploading it to the Librarian repo as part of the config file. You have been
  warned.)

With all of these config options defined, the Librarian will attempt to use
Globus to transfer files. If not all of these keys are defined, it will fall
back on `rsync` for transfers. Additionally, the Librarian will fall back on
`rsync` on a per-file basis if the Globus transfer fails for any reason, so the
Librarian is not wholly dependent on Globus even if configured to rely primarily
on it.

A sample server config file may look in part like:
```json
{
    "server": "tornado",
    "use_globus": true,
    "globus_client_id": "224532bb-8a4b-4d32-8995-e1fb442be98e",
    "globus_transfer_token": "AQBX8YvVAAAAAAADxhAtF46RxjcFuoxN1oSOmEk-hBqvOejY4imMbZlC0B8THfoFuOK9rshN6TV7I0uwf0hb",
}
```

In addition to the mandatory keys outlined above, there is an optional key:
`globus_endpoint_id`. This may be specified if there is a single Globus endpoint
ID for each store initiating an upload (e.g., all stores are located on the same
machine). The Librarian will use this endpoint ID instead of inferring the local
Endpoint ID using the `LocalGlobusConnectPersonal` class as above. This option
should *not* be specified if different stores have different Endpoint IDs,
instead relying on the `LocalGlobusConnectPersonal.endpoint_id` attribute to
provide the correct Endpoint ID.

### Remote Librarian Configuration

In addition to the Librarian server configuration, the list of remote hosts must
have their Globus information specified. Config keys that will be used for this
purpose are:

- `globus_endpoint_id`: the Globus Endpoint ID that should be used for the
  destination.
- `globus_host_path` (optional): if the destination Globus Endpoint is a Shared
  Endpoint (discussed more [below](#globus-shared-endpoints)), the path to the
  root exposed directory.

For example, a `~/.hl_client.cfg` file with Globus information may look like:
```json
{
    "connections": {
        "remote-globus-host": {
            "url": "http://remote-host.com:21106",
            "authenticator": "my_authenticator",
            "globus_endpoint_id": "b19b3b45-01ae-11e6-a71c-22000bf2d559",
            "globus_host_path": "/export/data"
        }
    }
}
```

When initiating a transfer to a remote Librarian, the client will use this
information to construct the transfer appropriately.


## Using Globus From the Librarian Command-Line Interface

In addition to using Globus for progrommatic uploads as part of a Standing
Order, it is possible to upload individual files to remote Librarian instances
using Globus. Additional information can be found by running `librarian upload
-h`. Here we provide some notes on upload options specific to Globus.

- `--use_globus`: option to indicate that the client should try to use Globus
  for the upload.
- `--client_id`: the Globus Client ID that should be used. In general this is
  the same as the one that would be specified in the server config file.
- `--transfer_token`: the Globus Refresh Token that should be used. As with the
  `client_id`, this is generally the same as the one in the server config.
- `--source_endpoint_id`: optional, again to specify the local Endpoint ID that
  should be used. If not specified, it will be inferred using the
  `LocalGlobusConnectPersonal` class provided by the `globus_sdk` module.


## Globus Shared Endpoints

Server Endpoints in Globus typically require some type of user authentication to
use. This authentication becomes cumbersome when attempting to run in a
programmatic or automated fasion. To address this issue, the Globus
infrastructure provides the concept of a "Shared Endpoint". Essentially the
Shared Endpoint behaves in a similar manner to a Personal Endpoint, with
optional restrictions on which Globus users are allowed to access it. For help
with creating a Shared Endpoint on the destination machine for Globus transfers,
please contact the system administrators of your cluster.

Once a Shared Endpoint has been created, it typically only exposes a limited
subset of the machine's filesystem. This information is listed under the "Host
Path" of the endpoint's Overview page in Globus. When a destination location for
a Librarian remote transfer is generated, the full absolute path is constructed
(because when performing a transfer using `rsync`, the absolute path is required
to ensure the file arrives where expected). To convey the information that the
file should be copied to the location *relative to the Shared Endpoint root* as
opposed to the filesystem root, the `globus_host_path` config option is
used. This ensures that the first part of the destination path matching the
Shared Endpoint's Host Path is removed from the destination file, so that the
file is transferred to the target location.
