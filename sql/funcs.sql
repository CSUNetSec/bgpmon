/* queries for a given collector time between two times (inclusive) and returns the names of the 
 * tables that contain info as rows. Expects tha main table to be named "dbs"
 */
create or replace function tablesFor(col text, t1 timestamp, t2 timestamp) returns setof text as $$
declare
r text;
begin
for r in select dbname from dbs where dbs.datefrom >= t1 AND dbs.dateto <= t2 AND dbs.collector = col
loop
return next r;
end loop;
return;
end
$$
language plpgsql;
