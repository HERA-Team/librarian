/* schema for Librarian DB */
/* see http://herawiki.berkeley.edu/doku.php/librarian for details */

create table source (
    id              SERIAL,
    name            varchar(254)    not null,
    authenticator   varchar(254)    not null,
    create_time     timestamp          not null,
    primary key (id)
);

create table observation (
    obsid              bigint          not null,
    start_time_jd     double precision not null,
    stop_time_jd     double precision,
    lst_start_hr     double precision,
    primary key (obsid)
);

create table store (
    id              SERIAL,
    name            varchar(254)    not null,
    create_time     timestamp          not null,
    rsync_prefix    varchar(254)    not null,
    http_prefix     varchar(254)    not null,
    path_prefix     varchar(254)    not null,
    ssh_prefix      varchar(254)    not null,
    unavailable     smallint         not null,
    primary key (id)
);

create table file (
    id           SERIAL,                    -- unique identifier for file, primary key
    name         varchar(254)     not null, -- file name
    type         varchar(64)      not null, -- file type
    create_time  timestamp        not null, -- file creation time in librarian
    obsid        bigint           not null, -- observation id from M&C
    source_id    integer          not null, -- where the file came from (foreign key into source table)
    store_id     integer          not null, -- where the file is stored
    size         double precision not null, -- file size in bytes
    md5          varchar(254)     not null, -- md5 hash
    deleted      smallint         not null, -- boolean flag for deleted files (0=not deleted, 1=deleted)
    deleted_time timestamp        not null, -- time file was deleted in librarian
    primary key (id)
);

create table history (
    id          SERIAL,       -- unique primary key
    create_time timestamp,    -- when this history item was created
    file_id     integer,      -- the file that this item refers to
    type        varchar(254), -- general classification of this item; of the form "rtp.processed"
    payload     varchar(512), -- extra data; interpretation depends on "type"
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
