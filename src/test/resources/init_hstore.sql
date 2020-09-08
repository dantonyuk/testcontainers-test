create extension hstore;

create table dict (
    data hstore
);

insert into dict values (hstore(array['a', '1', 'b', '2', 'c', '3']));
