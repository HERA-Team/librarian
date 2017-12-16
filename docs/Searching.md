# HERA Librarian: Searching

The Librarian lets you search for files and observations. This page documents
how to compose search queries.

**Contents**

- [Overview](#overview)
- [Generic search clauses](#generic-search-clauses)
- [Searching on attributes](#searching-on-attributes)
- [Searching for files](#searching-for-files)
- [Searching for observations](#searching-for-observations)
- [Searching for sessions](#searching-for-sessions)


## Overview

The Librarian lets you perform highly *structured* searches for files,
observations, or sessions. You design searches with very specific match terms,
such as “this file has at least 3 known instances on this server,” and then
the Librarian executes your query and tells you what items matched your
requirements.

Librarian searches are typed as [JSON](http://www.json.org/) data structures.
This format is fairly straightforward, but a bit rigid. An example search
might be:

```
{
  "name-matches": "zen.2457644.%.xx.HH.uvc",
  "num-instances-greater-than": 0,
}
```

As a special extension, you can include comments in your searches by putting
text after a hash mark (`#`). Unfortunately, the JSON parser we use doesn't
give good error messages if it doesn’t like what you’ve typed in. Use
[the online JSON validator](https://jsonlint.com/) if it looks like there’s a
problem with the syntax of what you’re typing.

The example search above has two *clauses*. They are joined with a logical
“AND”: both have to be true for a file to match the query. Librarian searches
work under Boolean logic: the search is essentially a logical statement, and
the files that match the search are exactly the set of files for which the
statement evaluates to `True`. As you might hope, there are ways to get
logical “OR” and “NOT” behavior as well — see below.

Clauses always have a *name* (such as `name-matches`) and they have to have
some kind of *payload* (above, `"zen.2457644.%.xx.HH.uvc"`) that gives a piece
of search information. (There are some clauses that don’t technically *need* a
payload, but JSON syntax demands that you put something.)

Most search clauses filter on *attributes* of the items you’re searching for.
For instance, every file has an attribute called `size` that is the size of
that file, measured in bytes. There are clauses named `size-is-exactly`,
`size-greater-than`, and `size-in-range` — among others — that match files
whose `size` attributes have the specified characteristics.

The Librarian provides different user interfaces depending on what kind of
item you’re searhing for, and different kinds of clauses are available
depending on what kind of thing your searching for. The results can be
presented in different ways, too, depending on what you’re looking for.


## Generic search clauses

Here we describe some clauses that are generic and don’t have anything to do
with any particular kind of thing that you’re searching for.

#### and

An `and` clause contains an arbitrary list of sub-clauses, and it evaluates to
`True` if *all* of those sub-clauses evaluate to `True`. The payload is a JSON
dictionary (`{}`) of those sub-clauses.

The query

```
{
   "and": {
      "name-matches": "%.bad_ants",
      "num-instances-greater-than": 2,
   }
}
```

matches all files whose names end in `.bad_ants` that have at least three
instances known to the Librarian.

The “outside” of every Librarian search is basically an `and` clause, so it
is not often necessary to explicitly use one.

#### or

An `or` clause contains an arbitrary list of sub-clauses, and it evaluates to
`True` if *any* of those sub-clauses evaluate to `True`. The payload is a JSON
dictionary (`{}`) of those sub-clauses.

The query

```
{
   "or": {
      "size-greater-than": 5000000000,
      "name-is-exactly": "zen.2458002.22551.xx.uv",
   }
}
```

matches all files with sizes larger than about 5 GB as well as the file whose
name is `zen.2458002.22551.xx.uv`.

Be careful with `or` clauses, since a sub-clause that matches lots of files
can easily lead to a search that gives an absurdly large number of matches,
which can be hard on the server.

#### none-of

A `none-of` clause contains an arbitrary list of sub-clauses, and it evaluates to
`True` if *none* of those sub-clauses evaluate to `True`. The payload is a JSON
dictionary (`{}`) of those sub-clauses.

The query

```
{
  "none-of": {
      "name-matches": "zen.%"
   }
}
```

matches all files whose names do not begin with `zen.`. This example shows how
`none-of` can be used to achieve logical negation (“NOT”), by giving it a
single sub-clause.

#### always-true

An `always-true` clause always evaluates to `True`. Sometimes this comes in
handy. The payload is ignored.

The query

```
{
  "none-of": {
    "always-true": []
  }
}
```

will never match any files.

#### always-false

An `always-false` clause always evaluates to `False`. Sometimes this comes in
handy. The payload is ignored.

The query

```
{
  "always-false": []
}
```

will never match any files.


## Searching on attributes

As mentioned above, most clauses give a constraint on some kind of named
*attribute* of the kind of thing you’re searching for: “file’s size is larger
than XX gigabytes”, or “observation’s start LST is between YY and ZZ”.

Each attribute has a *type*, corresponding to that of its underlying Python
variable. The currently recognized types are *strings*, *integers*, and
*floating-point* numbers, all with the usual meanings. For each attribute with
a given type, a standard set of clauses is available. We describe these below.

#### {attribute}-is-exactly

Tests that the specified attribute has exactly the given value. *This is only
available for string- and integer-typed attributes.*

For example:

```
{
   "size-is-exactly": 12345
}
```

or

```
{
  "name-is-exactly": "zen.2458002.22551.xx.uv"
}
```

You can’t search for exact values of floating-point attributes due to
difficulties with rounding and precision. If you want to search for a very
precise value of a float attribute, use the `-in-range` clause with a very
restrictive range.

#### {attribute}-is-not

The opposite of `{attribute}-is-exactly`: returns `True` if the attribute
value is *not* exactly equal to the specified value. Use carefully since this
will match lots of files.

```
{
   "size-is-not": 2,
   "name-matches": "%.bad_ants"
}
```

#### {string-attribute}-matches

Checks that a string-typed attribute matches a textual template using the
[SQL LIKE](https://www.w3schools.com/sql/sql_like.asp) operator. This is
essentially like a “glob” on Unix, but using a different syntax: `%` matches
any string of characters and `_` matches any single character. Therefore

```
{
   "name-matches": "zen.24572%xx.HH.uv"
}
```

matches files whose names start with the text `zen.24572` and end with the
text `xx.HH.uv`. To search for a file whose name starts with `blooga`, use:

```
{
   "name-matches": "blooga%"
}
```

### {number-attribute}-greater-than

Checks that an int- or float-typed attribute is strictly greater than the
payload value.

```
{
   "size-greater-than": 5000000000
}
```

Note that there is no greater-than-or-equal operator, but the
`{attribute}-in-range` clause has inclusive limits.

### {number-attribute}-less-than

Checks that an int- or float-typed attribute is strictly less than the
payload value.

```
{
   "num-instances-less-than": 1
}
```

Note that there is no less-than-or-equal operator, but the
`{attribute}-in-range` clause has inclusive limits.

### {number-attribute}-in-range

Checks that an int- or float-typed attribute is between two values. The
payload is a JSON list (`[]`) of the two limits:

```
{
   "size-in-range": [100, 200]
}
```

This searches for files whose sizes are between 100 and 200 bytes,
*inclusive*. That is, files whose sizes are exactly 100 or exactly 200 bytes
will match this clause.

### {number-attribute}-not-in-range

The logical negation of `{attribute}-in-range`.

```
{
   "size-not-in-range": [100, 5000000000]
}
```

This matches files with sizes of 99 bytes or fewer, or 5000000001 bytes or
more.


## Searching for files

When searching for files, you can query the following attributes, using the
generic clause types described in
[the previous section](#searching-on-attributes).

| Name | Type | Description |
| :--- | :--- | :---------- |
| name | string | The file’s name |
| type | string | The file’s “type”, which is the piece of its name after the last `.` |
| source | string | The name of the “source” that told the Librarian about the file |
| size | int | The file’s size in bytes |
| obsid | int | The obsid with which the file is associated |
| num-instances | int | The number of instances of this file on this Librarian |
| start-time-jd | float | The JD at which this file’s observation started |
| stop-time-jd | float | The JD at which this file’s observation ended |
| start-lst-hr | float | The LST at which this file’s observation started, in hours |
| session-id | int | The session-ID with which this file is associated |

Not all of these attributes are known for every file. For instance, not all
files are associated with a session at all. Files lacking a particular
attribute will never match a query that conditions on that attribute.

To find all raw XX polarized data files with a very particular start LST, you
might write:

```
{
  "start-lst-hr-in-range": [12.00, 12.01],
  "name-matches": "%.xx.uv"
}
```

There are also a few clauses that you can specify that do not fall into the
attribute-matching schema described above. They are:

#### not-older-than

Matches files whose logged creation time is more recent than a certain number
of days ago. The search

```
{
   "not-older-than": 3
}
```

matches files registered with the Librarian within the last three days.

Note that the “creation time” is a time that a file was registered with the
Librarian. It is not necessarily the time that an observation was made, if the
file is a UV data set, or the creation time of the file according to the Unix
filesystem.

#### not-newer-than

Matches files whose logged creation time is more than a certain number of days
ago. The search

```
{
   "not-newer-than": 3,
   "not-older-than": 7
}
```

matches files registered with the Librarian more than 3 days ago, but less
than 7 days ago.

Note that the “creation time” is a time that a file was registered with the
Librarian. It is not necessarily the time that an observation was made, if the
file is a UV data set, or the creation time of the file according to the Unix
filesystem.

### obs-matches

Matches files associated with an *observation* that matches the specified
sub-query. That is, the payload should be a query that matches observations,
not files. For instance,

```
{
  "name-like": "%.xx.uv",
  "obs-matches": {
    "obsid-in-range": [1164855000, 1164856000],
    "num-files-is-not": 2592
  }
}
```

matches files with names ending in `.xx.uv` in observations whose obsids are
in the specified range that do not contain 2592 files.

Note that you can query files for observation-based quantities like
`start-lst-hr` directly, so this clause is only needed if you have more
sophisticated constraints that you want to apply.

### obsid-is-null

Matches “maintenance” files that are not associated with any observation. They
payload is ignored. For instance,

```
{
  "name-like": "%.pgdump",
  "obsid-is-null": ""
}
```

matches all maintenance files whose names end in `.pgdump`.


## Searching for observations

When searching for observations, you can query the following attributes, using
[the generic clause types described in above](#searching-on-attributes).

| Name | Type | Description |
| :--- | :--- | :---------- |
| obsid | int | The observation’s obsid |
| start-time-jd | float | The JD at which this observation started |
| stop-time-jd | float | The JD at which this observation ended |
| start-lst-hr | float | The LST at which this observation started, in hours |
| session-id | int | The session-ID with which this observation is associated |
| duration | float | The duration of this observation, in days |
| num-files | int | The number of individual files associated with this observation |
| total-size | int | The total size of all files associated with this obs, in bytes |

Not all of these attributes are known for every observation. For instance,
observations from the 2016-17 season do not have a stop time or a duration.
Observations lacking a particular attribute will never match a query that
conditions on that attribute.

To find all observations with a very particular start LST, you might write:

```
{
  "start-lst-hr-in-range": [12.00, 12.01],
}
```


## Searching for sessions

When searching for observing sessions, you can query the following attributes, using
[the generic clause types described in above](#searching-on-attributes).

| Name | Type | Description |
| :--- | :--- | :---------- |
| session-id | int | The sessions’s unique identifier number |
| start-time-jd | float | The JD at which this session started |
| stop-time-jd | float | The JD at which this session ended |
| duration | float | The duration of this session, in days |
| age | float | How many days have elapsed since the end of the session |
| num-obs | int | The number of individual observations associated with this session |
| num-files | int | The number of individual files associated with this session |

To find all sessions containing a very particular number of observations, you might write:

```
{
  "num-obs-is-exactly": 57,
}
```

There are also a few clauses that you can specify that do not fall into the
attribute-matching schema described above. They are:

#### no-file-has-event

Matches sessions that contain *no* files that have an associated “file event” of
a certain specified type. For instance:

```
{
   "no-file-has-event": "create_instance"
}
```

will match only sessons for which every single associated file has never had any
instances on this Librarian, since creating an instance of a file causes a
`create_instance` event to be logged.

This is a highly targeted search used to implement our nightly data analysis
cron job. It is slow.
