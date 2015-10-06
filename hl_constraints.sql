alter table user
    add unique(name);

alter table site
    add unique(name);

alter table store
    add unique(site_id, name),
    add foreign key(site_id) references site(id);

alter table file
    add unique(name),
    add foreign key(user_id) references user(id);

alter table file_instance
    add foreign key(store_id) references store(id),
    add foreign key(file_id) references file(id),
    add foreign key(user_id) references user(id);
