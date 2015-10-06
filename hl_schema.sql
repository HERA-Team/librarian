create table user (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    authenticator   varchar(254)    not null,
    create_time     double          not null,
    primary key (id)
) engine=InnoDB;

create table site (
    id              integer         not null auto_increment,
    name            varchar(254)    not null,
    create_time     double          not null,
    primary key (id)
) engine=InnoDB;

create table store (
    id              integer         not null auto_increment,
    site_id         integer         not null,
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
    size            double          not null,
    user_id         integer         not null,
    md5             varchar(254)    not null,
    primary key (id)
) engine=InnoDB;

create table file_instance (
    id              integer         not null auto_increment,
    file_id         integer         not null,
    store_id        integer         not null,
    create_time     double          not null,
    user_id         integer         not null,
    primary key (id)
) engine=InnoDB;
