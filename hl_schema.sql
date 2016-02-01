/* schema for Librarian DB */

create table source (
    id              SERIAL,
    name            varchar(254)    not null,
    authenticator   varchar(254)    not null,
    create_time     timestamp          not null,
    primary key (id)
);

/* clone of M&C table */

create table observation (
    id              bigint          not null,
    source_id       integer         not null,
    julian_date     double precision          not null,
    polarization    char(4)         not null,
    length          double precision          not null,
    primary key (id)
);

create table store (
    id              SERIAL,
    name            varchar(254)    not null,
    create_time     timestamp          not null,
    capacity        double precision          not null,
    used            double precision          not null,
    rsync_prefix    varchar(254)    not null,
    http_prefix     varchar(254)    not null,
    path_prefix     varchar(254)    not null,
    ssh_prefix      varchar(254)    not null,
    unavailable     smallint         not null,
    primary key (id)
);

create table file (
    id              SERIAL,
    name            varchar(254)    not null,
    type            char(64)        not null,
    create_time     timestamp          not null,
    obs_id          bigint          not null,
    source_id       integer         not null,
    store_id        integer         not null,
    size            double precision          not null,
    md5             varchar(254)    not null,
    deleted         smallint         not null,
    deleted_time    timestamp          not null,
    primary key (id)
);

create table task (
    id              SERIAL,
    create_time     timestamp          not null,
    task_type       integer         not null,
    local_store_id  integer         not null,
    file_name       varchar(254)    not null,
    remote_site     varchar(254)    not null,
    remote_store    varchar(254)    not null,
    in_progress     smallint         not null,
    delete_when_done    smallint     not null,
    state           integer         not null,
    completed       smallint         not null,
    completed_time  timestamp          not null,
    last_error      varchar(254)    not null,
    last_error_time timestamp          not null,
    primary key (id)
);

