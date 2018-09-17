## Clear schema

drop schema public cascade;
create schema public;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO public;