# constraints for M&C DB

alter table observation
    add foreign key(source_id) references source(id);

alter table status
    add foreign key(observation_id) references observation(id),
    add foreign key(source_id) references source(id);
