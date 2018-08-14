# Ingesting New Files

Say you have created a shiny new data file that deserves to be enshrined in
the Librarian. How do you make that happen?

You should use the [upload_to_librarian.py](../scripts/upload-to-librarian.py)
program. This tool uses the Librarian “client” modules, so you need to set
those up first as described in
[Programmatic Access to the Librarian](#programmatic-access-to-the-librarian).

After installing the client modules, the
[upload_to_librarian.py](../scripts/upload-to-librarian.py) script should be
visible in your path. You can run it with the `--help` option to see its
self-contained help. The basic usage format is:

```
upload_to_librarian.py {connection} {local-path} {dest-path}
```

For instance:

```
upload_to_librarian.py human@folio zen.2557561.66007.phases.hdf 2557561/zen.2557561.66007.phases.hdf
```

*The name of your file is important for several reasons*. First, obviously,
the name is permanent and unchangeable, so once you upload a file you’re stuck
with it.

Second, the vast majority of files should be associated with an obsid
(observation identifier). By default, the uploader script will infer that
obsid by matching it with existing file sharing the same prefix —
specifically, the same text before the *third* period (.) in the the filename.
That would be `zen.2557561.66007` in this case. If nothing matches, your
upload will be rejected. (It is possible, to override this default inference
behavior using the not-really-documented `--meta=json-stdin` option, but this
is not something that regular users should have to worry about.)

Third, each file has a “type” associated with it, which is a short string
describing its format. By default, the uploader script guesses it from the
final extension of the filename — the type would be “hdf” in the example
above. We don’t use these types right now, but please make sure the extension
is sensible.

Finally, you might recall that we said that Librarian filenames may not
contain directory parts — “/” characters. But in the example above, the
destination path has one! What gives? What’s happening here is that the
directory piece is a hint to the Librarian of what directory to stash the file
in *in its local storage*. The Official Librarian Name of the uploaded file
will always be the final piece of whatever path you give it, but this helps
keeps the files organized sensibly on the storage disks. By convention, files
are stored in directories named according to the integer part of their JD.
Also, note that the name of your file on disk does not need to match the name
that it will be given on the Librarian.


## Uploading Files Without an Obsid

As mentioned above, the Librarian defaults to a model in which each file is
associated with an obsid. However, there are some files for which this doesn’t
make sense — e.g., database backup files, as mentioned in the
[Administration](./Administration.md) page.

You can upload such a file by adding the `--null-obsid` option to the
`upload_to_librarian.py` command. By making you have to explicitly indicate
that you want your file to be unassociated with an obsid, we prevent
accidental ingestion of files that *should* have an obsid but for which the
matching scheme identified above failed.
