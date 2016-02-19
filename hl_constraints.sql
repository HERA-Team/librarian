/* constraints for Librarian DB */

create unique index on source(name);

alter table observation add constraint osourcefk foreign key (source_id) references source(id);

create unique index on store(name);

create unique index on file(name, store_id);
alter table file add constraint fstorefk foreign key (store_id) references store(id);
alter table file add constraint fobsfk foreign key (obs_id) references observation(id);
alter table file add constraint fsourcefk foreign key (source_id) references source(id);

alter table history add constraint hfilefk foreign key (file_id) references file(id);

create unique index on task(local_store_id, file_name, remote_site, remote_store);
alter table task add constraint tstorefk foreign key (local_store_id) references store(id);
