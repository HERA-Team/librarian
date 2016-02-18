insert into store (
  name, create_time, capacity, used, rsync_prefix, http_prefix, path_prefix, ssh_prefix, unavailable
) values
  /* 100 GiB capacity; rsync pots do not run httpd but set http_prefix anyway */
  ('data6', NOW(), 38909233648, 131092, 'hera@enterprise:/data6', 'http://localhost/data6',
   '/data', 'hera@enterprise', 0);
