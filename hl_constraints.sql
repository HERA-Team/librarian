# constraints for Librarian DB

alter table source
    add unique(name);

alter table store
    add unique(name);

alter table file
    add unique(name, store_id),
    add foreign key(store_id) references store(id);
