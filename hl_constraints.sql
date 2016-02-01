/* constraints for Librarian DB */

create unique index on source(name);

create unique index on store(name);

create unique index on file(name, store_id);
alter table file add constraint fstorefk foreign key (store_id) references store(id);

create unique index on task(local_store_id, file_name, remote_site, remote_store);
alter table task add constraint tstorefk foreign key (local_store_id) references store(id);
