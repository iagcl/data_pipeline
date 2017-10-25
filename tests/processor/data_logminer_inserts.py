import collections

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statement input_primary_key_fields expected_fields expected_values")

tests=[

  TestCase(
    description="Real-world LogMiner redo statement example",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DEVT_TAG_ID","DETN_EVNT_ID","TAG_INTL_NUM","TAG_EXT_NUM","TAG_CLS","TAG_STAT","ISSR_CODE","BLK_LST_CODE","LAST_UPD","DSRC_STATUS") values ('44364452346811','20161119452346810','0270815294','1009020542','2','0000','201','Z',TO_TIMESTAMP('19-NOV-2016 11:30:53.0 PM'),',Complete,') """, 
    input_primary_key_fields = None,
    expected_fields=["DEVT_TAG_ID","DETN_EVNT_ID","TAG_INTL_NUM","TAG_EXT_NUM","TAG_CLS","TAG_STAT","ISSR_CODE","BLK_LST_CODE","LAST_UPD","DSRC_STATUS"], 
    expected_values=['44364452346811','20161119452346810','0270815294','1009020542','2','0000','201','Z','19-NOV-2016 11:30:53.0 PM',',Complete,'])

, TestCase(
    description="Single string value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('Z')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=['Z'])

, TestCase(
    description="NULL value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A") values (NULL)""", 
    input_primary_key_fields = None,
    expected_fields=["A"], 
    expected_values=[None])

, TestCase(
    description="NULL surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a',NULL,'c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', None, 'c'])

, TestCase(
    description="'NULL' string surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a','NULL','c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', 'NULL', 'c'])

, TestCase(
    description="Non-null surrounded by NULL value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values (NULL,'b',NULL)""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=[None, 'b', None])

, TestCase(
    description="Non-null surrounded by 'NULL' string values",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('NULL','b','NULL')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['NULL', 'b', 'NULL'])

# EMPTY_BLOB tests

, TestCase(
    description="EMPTY_BLOB() value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A") values (EMPTY_BLOB())""", 
    input_primary_key_fields = None,
    expected_fields=["A"], 
    expected_values=[None])

, TestCase(
    description="EMPTY_BLOB() surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a',EMPTY_BLOB(),'c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', None, 'c'])

, TestCase(
    description="EMPTY_BLOB() string surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a','EMPTY_BLOB()','c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', 'EMPTY_BLOB()', 'c'])

, TestCase(
    description="Non-null surrounded by EMPTY_BLOB() value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values (EMPTY_BLOB(),'b',EMPTY_BLOB())""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=[None, 'b', None])

, TestCase(
    description="Non-null surrounded by EMPTY_BLOB() string values",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('EMPTY_BLOB()','b','EMPTY_BLOB()')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['EMPTY_BLOB()', 'b', 'EMPTY_BLOB()'])


# EMPTY_CLOB tests
, TestCase(
    description="EMPTY_CLOB() value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A") values (EMPTY_CLOB())""", 
    input_primary_key_fields = None,
    expected_fields=["A"], 
    expected_values=[None])

, TestCase(
    description="EMPTY_CLOB() surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a',EMPTY_CLOB(),'c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', None, 'c'])

, TestCase(
    description="EMPTY_CLOB() string surrounded by non-null value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('a','EMPTY_CLOB()','c')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['a', 'EMPTY_CLOB()', 'c'])

, TestCase(
    description="Non-null surrounded by EMPTY_CLOB() value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values (EMPTY_CLOB(),'b',EMPTY_CLOB())""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=[None, 'b', None])

, TestCase(
    description="Non-null surrounded by EMPTY_CLOB() string values",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("A","B","C") values ('EMPTY_CLOB()','b','EMPTY_CLOB()')""", 
    input_primary_key_fields = None,
    expected_fields=["A", "B", "C"], 
    expected_values=['EMPTY_CLOB()', 'b', 'EMPTY_CLOB()'])

, TestCase(
    description="Comma and single quotes",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('Complete, with comma and ''quote''')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=["Complete, with comma and 'quote'"])

, TestCase(
    description="Comma and single quotes after value and before comma",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('Z '', single quote after Z before comma')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=["Z ', single quote after Z before comma"])

, TestCase(
    description="Trailing spaces at the end of redo statement",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('Trailing spaces')    """, 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=["Trailing spaces"])

, TestCase(
    description="Single quote inside value",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('''')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=["'"])

, TestCase(
    description="Single quote starting, followed by comma",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values (''', single quote only before comma')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=["', single quote only before comma"])

, TestCase(
    description="Function string within quotes should not be stripped",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("DSRC_STATUS") values ('FUNCTION_LOOKALIKE(something)')""", 
    input_primary_key_fields = None,
    expected_fields=["DSRC_STATUS"], 
    expected_values=['FUNCTION_LOOKALIKE(something)'])

, TestCase(
    description="Tricky string with everything put together",
    input_table_name="TT_DEVT_TAG", 
    input_commit_statement="""insert into "DEDB"."TT_DEVT_TAG"("BLK_LST_CODE","LAST_UPD","DSRC_STATUS") values ('Z,'',,''''',NULL,TO_TIMESTAMP('19-NOV-2016 ,'' '', 11:30:53.0 PM'),',Complete,FunctionLookalike(@!$%#^&TrickyString0987597)', '"string with double-quote"')""", 
    input_primary_key_fields = None,
    expected_fields=["BLK_LST_CODE","LAST_UPD","DSRC_STATUS"], 
    expected_values=["Z,',,''", None, "19-NOV-2016 ,' ', 11:30:53.0 PM", ',Complete,FunctionLookalike(@!$%#^&TrickyString0987597)', '"string with double-quote"'])

, TestCase(
    description="Single value with function containing 2 parameters",
    input_table_name="MY_TABLE_2", 
    input_commit_statement='insert into "MY_SCHEMA"."MY_TABLE_2"("START_DATE") values (TO_DATE(\'2017-06-06 15:36:24\', \'YYYY-MM-DD HH24:MI:SS\'))', 
    input_primary_key_fields = None,
    expected_fields=["START_DATE"], 
    expected_values=["2017-06-06 15:36:24"])

, TestCase(
    description="Single value with function containing 5 parameters with special chars",
    input_table_name="MY_TABLE_2", 
    input_commit_statement='insert into "MY_SCHEMA"."MY_TABLE_2"("START_DATE") values (TO_DATE(\'2017-06-06 15:36:24\', \'YYYY-MM-DD HH24:MI:SS\', \'param3withcomma,\', \'(param4withbrackets)\', \'(param5with,commas,andbracket\')))', 
    input_primary_key_fields = None,
    expected_fields=["START_DATE"], 
    expected_values=["2017-06-06 15:36:24"])

, TestCase(
    description="Statement with function containing > 1 parameter followed by NULL date",
    input_table_name="MY_TABLE_2", 
    input_commit_statement='insert into "MY_SCHEMA"."MY_TABLE_2"("TPA_ID","ASSET_DESC","POLICY_NUMBER","START_DATE","END_DATE","IS_VIEW","IS_AMEND","POLICY_SOURCE_SYSTEM") values (\'1023401\',\'2014 NISSAN ALMERA MCP0PVZY\',\'MOT017641188\',TO_DATE(\'2017-06-06 15:36:24\', \'YYYY-MM-DD HH24:MI:SS\'),NULL,\'Y\',\'Y\',\'HUON\')', 
    input_primary_key_fields = None,
    expected_fields=["TPA_ID","ASSET_DESC","POLICY_NUMBER","START_DATE","END_DATE","IS_VIEW","IS_AMEND","POLICY_SOURCE_SYSTEM"], 
    expected_values=['1023401','2014 NISSAN ALMERA MCP0PVZY','MOT017641188','2017-06-06 15:36:24',None,'Y','Y','HUON'])

, TestCase(
    description="Statement with EMPTY_BLOB() and URL",
    input_table_name="GTW_TIMERS", 
    input_commit_statement='insert into "MY_SCHEMA"."GTW_TIMERS"("TIMER_ID","EXPIRES","INFO","INTERVAL_DUR","STATUS","CALLBACK_TYPE","CALLBACK_URL","CALLBACK_CLASS") values (\'1376678\',\'1496875513488\',EMPTY_BLOB(),\'0\',\'0\',\'1\',\'java:global/cRE/GTW_EnactmentServiceEJB/ActivityRunner!com.gtnet.workflow.enactmentService.ejb.ActivityRunnerHome\',\'com.gtnet.workflow.enactmentService.ejb.ActivityRunnerHome\')', 
    input_primary_key_fields = None,
    expected_fields=["TIMER_ID","EXPIRES","INFO","INTERVAL_DUR","STATUS","CALLBACK_TYPE","CALLBACK_URL","CALLBACK_CLASS"],
    expected_values=['1376678','1496875513488',None,'0','0','1','java:global/cRE/GTW_EnactmentServiceEJB/ActivityRunner!com.gtnet.workflow.enactmentService.ejb.ActivityRunnerHome','com.gtnet.workflow.enactmentService.ejb.ActivityRunnerHome'])

, TestCase(
    description="Statement with parentheses in values",
    input_table_name="MY_TABLE", 
    input_commit_statement="""insert into "MY_SCHEMA"."MY_TABLE"("COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE") values ('1','124049','2010','LOTUS','EVORA',NULL,'COUPE','FI','35','(2 SEAT)','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0')""", 
    input_primary_key_fields = None,
    expected_fields=["COMPANY","SEQUENCE","MVYEAR","MVMAKE","MVMODEL","MVSERIES","MVBODY","MVENGTYP","MVENGCAP","MVEQUIP","STDEQUIP","MVTARE"],
    expected_values=['1','124049','2010','LOTUS','EVORA',None,'COUPE','FI','35','(2 SEAT)','ABB ABS AC  AL  AW1 CLR EBD EDL IM  LSW PM  PS  PW  RCD RS  SPS TC  TCS','0'])

]
