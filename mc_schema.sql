create table observation
    id              integer         not null auto_increment,
    julian_date     double          not null,
    polarization    char[4]         not null,
    length          double          not null,
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
    primary key (id)
) engine=InnoDB;
