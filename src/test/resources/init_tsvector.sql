create table words (
    data tsvector
);

insert into words values (to_tsvector('pg_catalog.english',
    'if you do what you’ve always done you’ll get what you’ve always gotten'));
