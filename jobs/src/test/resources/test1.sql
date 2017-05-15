create table if not exists test_ddl1_${granularity}_${pos} (
  id string,
  name string
) PARTITIONED BY (TIMEPOS STRING)
stored as textfile;

create table if not exists test_ddl2_${granularity}_${pos} (
  id string,
  name string
) PARTITIONED BY (TIMEPOS STRING)
stored as textfile;