# HERA Librarian: Server Administrator’s Notes

Here we collect some miscellaneous information that might be useful if you’re
responsible for running a Librarian server at a site.

### Contents

- [Monitoring with M&C](#monitoring-with-m_c)
- [Backing up the database](#backing-up-the-database)


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
`upload_to_librarian.py`.
