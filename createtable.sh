#my input is an parquet-format file
#and the create table words are here

sql="drop table databases_name.table_name;
create table databases_name.table_name(label string,v1 string,v2 string,v3 string,v4 string,v5 string,v6 string,v7 string,
v8 string,v9 string,v10 string,v11 string,v12 string,v13 string,v14 string,v15 string,v16 string,v17 string,v18 string,
v19 string,v20 string,v21 string,v22 string,v23 string,v24 string,v25 string,v26 string) stored as parquet;
"
sql2="insert into table sec_dw.drunk_merge_x_0
select *, 1 as tag from table1 union all select *, 2 as tag from table2
"
hive -e "set mapred.job.queue.name=  ;${sql}"
hive -e "set mapred.job.queue.name=  ;${sql2}"
