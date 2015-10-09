alter table source
    add unique(name);

alter table site
    add unique(name);

alter table store
    add unique(site_id, name),
    add foreign key(site_id) references site(id);

alter table file
    add unique(name),
    add foreign key(source_id) references source(id);

alter table file_instance
    add unique(file_id, store_id),
    add foreign key(store_id) references store(id),
    add foreign key(file_id) references file(id),
    add foreign key(source_id) references source(id);
