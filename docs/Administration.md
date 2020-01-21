# HERA Librarian: Server Administrator’s Notes

Here we collect some miscellaneous information that might be useful if you’re
responsible for running a Librarian server at a site.

### Contents

- [The Librarian connectivity model](#the-librarian-connectivity-model)
- [Monitoring with M&C](#monitoring-with-m_c)
- [Backing up the database](#backing-up-the-database)


## The Librarian Connectivity Model

An important aspect of the Librarian architecture is how it can be used to
replicate data and metadata between multiple sites. This section attempts to
give an overview of how Librarian instances connect to each other.

As alluded to in [the Overview](./Overview.md), a multiple-site Librarian
system is composed of multiple Librarian servers that occasionally communicate
with each other — it is *loosely coupled* and *decentralized*. There’s no
central master server that is the single source of truth (or single point of
failure).

Connections between Librarian instances are essentially unidirectional. In
particular, if a system contains two Librarian instances, *A* and *B*, *A* may
know how to initiate connections to *B* but that does not necessarily imply
that *B* knows how to initiate connections to *A*. It’s probably actually
relatively rare to have a Librarian network in which pairs of sites mutually
exchange data — the flow of data distribution is generally outward from a
central source (e.g., an observatory).

Librarian-to-Librarian connections are implemented with two categories of TCP
network connections. First, Librarians synchronize status information and
metadata through a fairly standard HTTP/JSON API, the same one that can be
accessed through the `hera_librarian` Python module. For Librarian *A* to be
able to communicate to Librarian *B*, there must be an entry for *B* in the
`"connections"` section of the JSON file `$HOME/.hl_client.cfg` on the host
running the main *A* server, where `$HOME` refers to the home directory of the
Unix user running the *A* Librarian server process(es). The
[Accessing](./Accessing.md) page has details about how connections are defined
in this file; in short, an example entry might be:

```
{
  "connections": {
    "librarian-B": {
      "url": "http://librarian-b.example.edu:21106/",
      "authenticator": "sampleauth"
    }
  }
}
```

This means that the server running *A* should be able to open an HTTP
connection to the host/port combination specified in the `url` parameter. The
`authenticator` is used for the Librarian’s primitive authentication scheme,
as described in [Accessing](./Accessing.md).

Importantly, because the Librarian system does not attempt to implement a
robust security scheme, it is not uncommon to want to tunnel inter-Librarian
connections over SSH. In that case, you’ll need to set up some kind of
standing SSH tunnel external to any Librarian software. In a typical tunnel
setup, the SSH command would resemble:

```
ssh -L 21110:localhost:21106 librarian-b.example.edu tail -f /dev/null
```

which means that a connection to the local port 21110 on the *A* host will be
forwarded over the SSH tunnel to `librarian-b.example.edu`, at which point it
will connect to `localhost:21106` *as seen from the B host*. The paired
`.hl_client.cfg` entry might be:

```
{
  "connections": {
    "librarian-B": {
      "url": "http://localhost:21110/",
      "authenticator": "sampleauth"
    }
  }
}
```

The [autossh](https://www.harding.motd.ca/autossh/) command may be helpful to
keep the tunnel active in the face of dropped connections and the like.

The second sort of TCP network connection used in inter-Librarian
communications are [rsync](https://rsync.samba.org/)-over-SSH connections used
for actual data transfer. Unlike the metadata connections, these are made
between *storage* nodes rather than Librarian server nodes. Another difference
is in how the connection destination is discovered. If Librarian *A* seeks to
transfer files to Librarian *B*, the process is as follows:

1. *A* connects to *B* using the JSON API described above and registers its
   intent to initiate a transfer.
2. *B* responds to *A*’s inquiry includes, among other things, an `ssh_host`
   field identifying one of its (*B*’s) storage nodes, which *A* relays to one
   of its storage nodes.
3. The *A* storage node initiates an rsync-over-SSH connection *B* storage
   node using the hostname given by the `ssh_host` field.

This means that if a Librarian is to receive data transfers, it must be
possible for potential source Librarians to directly connect to its storage
nodes over SSH. Furthermore, the same hostnames are provided to all Librarians
seeking to send data. Finally, SSH keys must be set up so that the source
stores can establish connections to the destination stores without requiring
interactive authentication.

All this is not as restrictive as it might seem because you can do a *lot* with
the [.ssh/config](https://www.ssh.com/ssh/config) file. Fields like `HostName`
or `ProxyCommand` allow the establishment of the SSH connection to be
customized and established in a more sophisticated way than a simple direct
TCP connection to a DNS name declared on the open Internet. Setting up such
configuration, however, does require that you synchronize it between all of
the Librarian *A* storage nodes, as that you well as make sure that any
relevant hostnames configured on Librarian *A* match those returned by
Librarian *B*.


## Monitoring with M&C

The version of the Librarian that lives on-site should report status
information to the HERA monitor-and-control (M&C) infrastructure. This can be
activated with the `report_to_mandc` setting in the server configuration file.
This reporting requires that the `hera_mc` Python module be available and that
the M&C configuration file `~/.hera_mc/mc_config.json` exists and is
configured properly.

Do *not* activate this feature unless you’re running the Karoo Librarian instance,
or you really know what you’re doing.


## Backing up the database

Manually backing up the database is easy! If you’re using Postgres, something
like this will do the trick:

```
pg_dump -F custom -f $SITENAME-$(date +%Y%m%d).pgdump $DB_NAME
```

The database can then be restored with `pg_restore`. These backups can be
ingested into the Librarian itself using the `--null-obsid` option of
`librarian upload`.
