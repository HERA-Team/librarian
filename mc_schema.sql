# schema for M&C database

create table source (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    authenticator   varchar(254)    not null,
    create_time     double          not null,
    primary key (id)
) engine=InnoDB;

create table observation (
    id              integer         not null auto_increment,
    julian_date     double          not null,
    polarization    char(4)         not null,
    length_days     double          not null,
    source_id       integer         not null,
    primary key (id)
) engine=InnoDB;

create table status (
    id              integer         not null auto_increment,
    observation_id  integer         not null,
    status          integer         not null,
    current_pid     integer         not null,
    still_host      char(100)       not null,
    still_path      char(100)       not null,
    output_host     char(100)       not null,
    output_path     char(100)       not null,
    source_id       integer         not null,
    primary key (id)
) engine=InnoDB;
