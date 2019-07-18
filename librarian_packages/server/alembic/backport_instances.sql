-- Copyright 2016 the HERA Collaboration
-- Licensed under the MIT License
--
-- Use this script to seed the "alembic_version" table on instances
-- of the Librarian database that have already been deployed. The
-- magic hex number is the Alembic revision identifier of the
-- "initial schema" that these deployed databases have been using.

CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL);
INSERT INTO alembic_version (version_num) VALUES ('71df5b41ae41');
