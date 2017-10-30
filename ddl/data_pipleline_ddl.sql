create sequence process_control_id_seq increment by 1 start with 1 no cycle ;
create sequence process_control_detail_id_seq increment by 1 start with 1 no cycle ;
create sequence source_system_profile_id_seq increment by 1 start with 1 no cycle ;
create sequence process_parameters_id_seq increment by 1 start with 1 no cycle ;
create sequence profile_id_seq increment by 1 start with 1 no cycle ;
create sequence reference_data_id_seq increment by 1 start with 1 no cycle ;
create sequence connections_id_seq increment by 1 start with 1 no cycle ;


create table process_control (
 id             	integer primary key default nextval('process_control_id_seq')
,profile_name           varchar(20)
,profile_version	integer
,process_code           varchar(20)
,process_name           varchar(30)
,source_system_code	varchar(30)
,source_system_type	varchar(10)
,source_region		varchar(256)
,target_system		varchar(30)
,target_system_type	varchar(10)
,target_region		varchar(256)
,process_starttime	timestamp without time zone
,process_endtime	timestamp without time zone
,min_lsn                varchar(30)
,max_lsn                varchar(30)
,status			varchar(20)
,duration		numeric(20,4)
,dml_count              bigint
,ddl_count              bigint
,other_count            bigint
,total_count            bigint
,comment		varchar(4000)
,filename               varchar(1024)
,infolog                varchar(1024)
,errorlog               varchar(1024)
,applier_marker         bigint                   
,object_list            text
);


create table process_control_detail (
 id             	integer primary key default nextval('process_control_detail_id_seq')
,run_id         	integer
,process_code           varchar(20)
,object_schema		varchar(30)
,object_name		varchar(50)
,process_starttime	timestamp without time zone
,process_endtime	timestamp without time zone
,status			varchar(20)
,source_row_count	bigint
,insert_row_count	bigint
,update_row_count	bigint
,delete_row_count	bigint
,bad_row_count		bigint
,alter_count		bigint
,create_count		bigint
,total_count            bigint
,duration		numeric(20,4)
,delta_starttime	timestamp without time zone
,delta_endtime		timestamp without time zone
,delta_startlsn		varchar(30)
,delta_endlsn		varchar(30)
,error_message		varchar(300)
,comment		varchar(300)
,filename               varchar(4000)
,linked_run_id          bigint                   
,query_condition	varchar(4000)
,infolog                varchar(1024)
,errorlog               varchar(1024)
);


create table source_system_profile (
 id                     integer primary key default nextval('source_system_profile_id_seq')
,profile_name           varchar(20)
,version                integer
,application_system     varchar(30)
,source_system_code     varchar(30)
,source_region          varchar(256)
,target_region          varchar(256)
,object_seq             bigint
,object_name            varchar(50)
,min_lsn                varchar(30)
,max_lsn                varchar(30)
,active_ind             varchar(1)
,history_ind            varchar(1)
,applied_ind            varchar(1)
,delta_ind              varchar(1)
,last_run_id            integer
,last_process_code      varchar(20)
,last_status            varchar(20)
,last_applied           timestamp without time zone
,last_history_update    timestamp without time zone
,last_updated           timestamp without time zone
,query_condition        varchar(4000)
,notes                  varchar(4000)
);


create table process_parameters (
 id             	integer primary key default nextval('process_parameters_id_seq')
,parameter_name		varchar(80)
,parameter_type		varchar(10)
,parameter_value	varchar(300)
);


create table reference_data (
 id                     integer primary key default nextval('reference_data_id_seq')
,domain                 varchar(50)
,code                   varchar(30)
,description            varchar(1000)
,active_ind             varchar(1)
,order_seq              integer
);

create table profile (
 id                     integer primary key default nextval('profile_id_seq')
,profile_name           varchar(20)
,version                integer
,source_system_code     varchar(30)
,source_database_type   varchar(30)
,source_connection      varchar(30)
,source_system          varchar(30)
,target_system_code     varchar(30)
,target_database_type   varchar(30)
,target_connection      varchar(30)
,target_system          varchar(30)
,description            varchar(1000)
,active_ind             varchar(1)
);


create table connections (
 id                     integer primary key default nextval('connections_id_seq')
,connection_name        varchar(20)
,connection_category    varchar(10)
,database_type          varchar(10)
,hostname               varchar(100)
,portnumber             integer
,username               varchar(50)
,password               varchar(50)
,database_name          varchar(50)
,created_by             varchar(50)
,created_date           timestamp without time zone
,updated_by             varchar(50)
,updated_date           timestamp without time zone
,notes                  varchar(200)
);


