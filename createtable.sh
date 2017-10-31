#my input is an parquet-format file
#and the create table words are here

sql="drop table databases_name.table_name;
create table databases_name.table_name(label int,v1 float,v2 float,v3 float,v4 float,v5 float,v6 float,v7 float,v8 float,v9 float,v10 float,v11 float,v12 float,v13 float,v14 float,v15 float,v16 float,v17 float,
v18 float,v19 float,v20 float,v21 float,v22 float,v23 float,v24 float,v25 float,v26 float) stored as parquet;
"
sql2="insert into table sec_dw.drunk_merge_x_0
select *, 1 as tag from table1 union all select *, 2 as tag from table2
"
hive -e "set mapred.job.queue.name=  ;${sql}"
hive -e "set mapred.job.queue.name=  ;${sql2}"
