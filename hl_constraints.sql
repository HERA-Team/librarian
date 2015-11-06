# constraints for Librarian DB

alter table source
    add unique(name);

alter table store
    add unique(name);

alter table file
    add unique(name, store_id),
    add foreign key(store_id) references store(id);

alter table task
    add unique(local_store_id, file_name, remote_site, remote_store),
    add foreign key(local_store_id) references store(id);
