create table service_for_blog(
  id integer primary key,
  uid char(6),
  train_category char(10),
  train_service_code   char(10),
  scheduleDayRuns char(7),
  schedule_start_date date,
  schedule_end_date date
 );
 
 
 create table service_segment_for_blob(
   service_id integer,
   tiploc varchar(40),
   departure char(6),
   arrival char(6)
 );