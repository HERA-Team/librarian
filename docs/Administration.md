# HERA Librarian: Server Administrator’s Notes

Here we collect some miscellaneous information that might be useful if you’re
responsible for running a Librarian server at a site.

### Contents

- [Backing up the database](#backing-up-the-database)


## Backing up the database

Manually backing up the database is easy! If you’re using Postgres, something
like this will do the trick:

```
pg_dump -F custom -f $SITENAME-$(date +%Y%m%d).pgdump $DB_NAME
```

The database can then be restored with `pg_restore`.
