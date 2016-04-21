insert into store (
  name, create_time, rsync_prefix, http_prefix, path_prefix, ssh_prefix, unavailable
) values
  /* rsync pots do not run httpd but set http_prefix anyway */
  ('data6', NOW(), 'hera@enterprise:/data6', 'http://localhost/data6',
   '/data', 'hera@enterprise', 0);
