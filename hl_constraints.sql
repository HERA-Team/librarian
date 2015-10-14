alter table source
    add unique(name);

alter table store
    add unique(name);

alter table file
    add unique(name),
    add foreign key(store_id) references store(id);
