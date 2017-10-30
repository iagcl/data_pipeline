# Data Pipeline

| [Overview](/README.md) | Initial Setup | [Design](/docs/design.md) |
|----|----|----|

## Overview

The following instructions are intended to provide a high-level overview of Setup steps


## Setup Instructions

1. Data Pipeline code installed including all dependancies on a Linux Server.
   - refer to README.md

     NB: If you do not have admin rights for server - contact a system administrator

   - Code installed in a release directory
       - e.g. /home/data_pipeline

   - A working directory configured with enough disk space to hold raw, data and log files
       - e.g. /data/da/<source_system>  

         where: 
         <source_system> represents a code/name for source system from which data will be replicated.

2. Kafka / Zookeeper node/cluster configured and in a working state (CDC only)
   - Create a single Kafka Topic for source system with one partition
       - e.g. CDC.<source_system>

         where: <source_system> represents a code/name for source system from which data 
         will be replicated.

     NB: Refer to Kafka documentation for setup and support instructions

3. If source database is Oracle - configure the following pre-requisites:
   - Ensure Archive Logging has been enabled.

   - Ensure Minimal Supplimental Logging has been enabled.

   - Create a user account used for data migration/replciation

   - The following privileges must be granted to a user account used for data migration when 
     using Oracle LogMiner for change data capture (CDC) with an Oracle source database:

     For Oracle versions prior to version 12c, grant the following:

     CREATE SESSION
     EXECUTE on DBMS_LOGMNR
     SELECT ANY TRANSACTION
     SELECT on V_$LOGMNR_LOGS
     SELECT on V_$LOGMNR_CONTENTS

     For Oracle versions 12c and higher, grant the following:

     LOGMINING (for example, GRANT LOGMINING TO <user account>)
     CREATE SESSION
     EXECUTE on DBMS_LOGMNR
     SELECT on V_$LOGMNR_LOGS
     SELECT on V_$LOGMNR_CONTENTS

   - On tables to be replicated, ensure supplimental logging is enabled for ALL columns.
     e.g. ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

   NB: Refer to Oracle documentation for further setup and support instructions


4. If source database is Microsoft SQLServer - confgure the following pre-requisites:
   - Before a capture instance can be created for individual tables, a member of the sysadmin fixed 
     server role must first enable the database for change data capture. 
     This is done by running the stored procedure below in the database context:

     sys.sp_cdc_enable_db 

     NB: This procedure must be executed for a database before any tables can be enabled 
         for change data capture in that database. 

   - Create a user account used for data replciation

   - On tables to be replicated, ensure the following stored procedure is executed:

     sys.sp_cdc_enable_table

   NB: Refer to Microsoft SQLServer documentation for further setup and support instructions


5. A postgres / greenplum target database for storing replicated data
   - Create a user account used for data replciation
   - Create schema(s) for landing data


6. A postgres database for storing data pipeline audit tables
   - Create a user account - sys_acqadm
   - Create the schema ctl to contain the pipeline process control audit tables
   - Ensure ctl schema is owned by user account
   - Execute the script process_control.ddl as the user account

   NB: Creation of this database is optional as the target database used for storing replciated data 
       could also contain the process control audit tables.


7. Configure initsync (full load) yaml template for source to target data acquisition
   - Take a copy of sample_initsync_config.yaml and rename to <source>_initsync_config.yaml
   - Adjust the following mandatory parameters:
     audituser
     auditschema (if different from ctl)
     sourceuser
     sourcedbtype
     sourceschema
     targetuser
     targetschema
     targetdbtype

     NB: Adjust other parameters as necessary to suit replication environment
    
8. If CDC is to be enabled then configure extractor / appleir yaml templates for source to target data replication
   - Take a copy of sample_extractor_config.yaml and rename to <source>_extractor_config.yaml
   - Take a copy of sample_applier_config.yaml and rename to <source>_applier_config.yaml
   - Adjust the following mandatory parameters:
     audituser
     auditschema (if different from ctl)
     sourceuser
     sourcedbtype
     sourceschema
     targetuser
     targetschema
     targetdbtype
     profilename
     profileversion
     streamhost
     streamchannel
     streamgroup
     streamschemahost
     streamschemafile

     NB: Adjust other parameters as necessary to suit replication environment


9. Initiate source database initsync (full load)
   - Create target DDL for source objects to be acquired

   - Create a source system acquisition profile
     Create a sql script inserting rows into ctl.source_system_profile
     One object (table / view) per insert

     Mandatory columns:

     - profile_name        
     - version             
     - source_system_code  
     - source_region (source schema)      
     - target_region (target schema)      
     - object_name (table / view name)        
     - active_ind (Y/N)         

     
   - Create a run script that contains the following command lines:

     PIPELINEBINDIR=$HOME/data_pipeline
     export LOG_CONFIG=$PIPELINEBINDIR/conf/logging.yaml

     python -m data_pipeline.initsync_pipe --config <workdir>/<source_system>_initsync_config.yaml

     where:
     <workdir> - represents the working directory containing the run script and raw / data / log files
     <source_system> represents a code/name for source system from which data will be replicated.


10. Initiate source database CDC Applier
   - Create a run script that contains the following command lines:

     PIPELINEBINDIR=$HOME/data_pipeline
     export LOG_CONFIG=$PIPELINEBINDIR/conf/logging.yaml

     python -m data_pipeline.apply --config <workdir>/<source_system>_applier_config.yaml

     where:
     <workdir> - represents the working directory containing the run script and raw / data / log files
     <source_system> represents a code/name for source system from which data will be replicated.

     NB: Amend logging.yaml to set correct logging level and redirection of stdout messaging


11. Initiate source database CDC extract
   - Create a run script that contains the following command lines:

     PIPELINEBINDIR=$HOME/data_pipeline
     export LOG_CONFIG=$PIPELINEBINDIR/conf/logging.yaml

     python -m data_pipeline.extract --config <workdir>/<source_system>_extractor_config.yaml

     where:
     <workdir> - represents the working directory containing the run script and raw / data / log files
     <source_system> represents a code/name for source system from which data will be replicated.

     NB: Amend logging.yaml to set correct logging level and redirection of stdout messaging


12. Start the WebUI Dashboard
   - Create a run script that contains the following command line:

     nohup python -m ui.app.app --audituser $AUDITUSER > ui.out 2>&1 &

     where:
     <AUDITUSER> - sys_acqadm:<password>@${AUDITHOST}:${AUDITPORT}/${AUDITDB}
     <AUDITHOST> - postgres database hostname containing process control audit tables
     <AUDITPORT> - postgres database port number (def: 5432)
     <AUDITDB>   - postgres database name (SID)
