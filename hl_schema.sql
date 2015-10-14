create table source (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    authenticator   varchar(254)    not null,
    create_time     double          not null,
    primary key (id)
) engine=InnoDB;

/* clone of M&C table */

create table observation (
    id              integer         not null,
    source_id       integer         not null,
    julian_date     double          not null,
    polarization    char(4)         not null,
    length_days     double          not null,
    primary key (id)
) engine=InnoDB;

create table store (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    create_time     double          not null,
    capacity        double          not null,
    used            double          not null,
    primary key (id)
) engine=InnoDB;

create table file (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    create_time     double          not null,
    observation_id  integer         not null,
    source_id       integer         not null,
    store_id        integer         not null,
    size            double          not null,
    md5             varchar(254)    not null,
    primary key (id)
) engine=InnoDB;
