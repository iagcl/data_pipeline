import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statement input_primary_key_fields expected_entries expected_sql")

tests=[
  TestCase(
    description="Add three fields, with surrounding brackets",
    input_table_name="als",
    input_commit_statement="""alter table als add ( c1 INTEGER , c2 INTEGER, c3 DATE )""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'c1', 'data_type': 'INTEGER', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'c2', 'data_type': 'INTEGER', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'c3', 'data_type': 'DATE', 'params': [], 'constraints': ''},
    ],
    expected_sql="ALTER TABLE als ADD c1 INTEGER, ADD c2 INTEGER, ADD c3 DATE"
  )

, TestCase(
    description="Add varchar2 with length",
    input_table_name="als",
    input_commit_statement="""alter table als add (comments varchar2(100))""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'comments', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE als ADD comments VARCHAR2(100)"
  )

, TestCase(
    description="Add operation, no surrounding brackets",
    input_table_name="als",
    input_commit_statement="""alter table als ADD clientnumber number(4)""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'clientnumber', 'data_type': 'NUMBER', 'params': ['4'], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE als ADD clientnumber NUMBER(4)"
  )

, TestCase(
    description="Single modify, no surrounding brackets",
    input_table_name="als",
    input_commit_statement="""alter table als modify comments varchar(300)""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.MODIFY, 'field_name': 'comments', 'data_type': 'VARCHAR', 'params': ['300'], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE als MODIFY comments VARCHAR(300)"
  )

, TestCase(
    description="Add and modify, no surrounding brackets, all caps",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD (C2 DATE, C3 TIMESTAMP), MODIFY C1 VARCHAR2(200)""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'C2', 'data_type': 'DATE', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'C3', 'data_type': 'TIMESTAMP', 'params': [], 'constraints': ''},
        {'operation': const.MODIFY, 'field_name': 'C1', 'data_type': 'VARCHAR2', 'params': ['200'], 'constraints': ''},
    ],
    expected_sql="ALTER TABLE ALS2 ADD C2 DATE, ADD C3 TIMESTAMP, MODIFY C1 VARCHAR2(200)"
  )

, TestCase(
    description="Add multiple, modify single, add multiple. Mixed case",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD (d2 DATE, d3 TIMESTAMP, a2 number(10,3), a3 number(12,2)), MODIFY C1 VARCHAR2(100), add (e1 DATE, e2 char(1))""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'd2', 'data_type': 'DATE', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'd3', 'data_type': 'TIMESTAMP', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'a2', 'data_type': 'NUMBER', 'params': ['10', '3'], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'a3', 'data_type': 'NUMBER', 'params': ['12', '2'], 'constraints': ''},
        {'operation': const.MODIFY, 'field_name': 'C1', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'e1', 'data_type': 'DATE', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'e2', 'data_type': 'CHAR', 'params': ['1'], 'constraints': ''},
    ],
    expected_sql="ALTER TABLE ALS2 ADD d2 DATE, ADD d3 TIMESTAMP, ADD a2 NUMBER(10, 3), ADD a3 NUMBER(12, 2), MODIFY C1 VARCHAR2(100), ADD e1 DATE, ADD e2 CHAR(1)"
  )

, TestCase(
    description="Add and modify, single operations in series, mixed case",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD f2 DATE, add f3 TIMESTAMP, MODIFY C1 VARCHAR2(300), add g1 DATE, add g2 char(1)""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'f2', 'data_type': 'DATE', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'f3', 'data_type': 'TIMESTAMP', 'params': [], 'constraints': ''},
        {'operation': const.MODIFY, 'field_name': 'C1', 'data_type': 'VARCHAR2', 'params': ['300'], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'g1', 'data_type': 'DATE', 'params': [], 'constraints': ''},
        {'operation': const.ADD, 'field_name': 'g2', 'data_type': 'CHAR', 'params': ['1'], 'constraints': ''},
    ],
    expected_sql="ALTER TABLE ALS2 ADD f2 DATE, ADD f3 TIMESTAMP, MODIFY C1 VARCHAR2(300), ADD g1 DATE, ADD g2 CHAR(1)"
  )

, TestCase(
    description="Add smallint",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD g4 smallint""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'g4', 'data_type': 'SMALLINT', 'params': [], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE ALS2 ADD g4 SMALLINT"
  )

, TestCase(
    description="Add float",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD g5 float""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'g5', 'data_type': 'FLOAT', 'params': [], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE ALS2 ADD g5 FLOAT"
  )

, TestCase(
    description="Add real",
    input_table_name="ALS2",
    input_commit_statement="""ALTER TABLE ALS2 ADD g6 real""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'g6', 'data_type': 'REAL', 'params': [], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE ALS2 ADD g6 REAL"
  )

, TestCase(
    description="Add outside bracket-bound single column",
    input_table_name="connct_cdc_pk2_cols10",
    input_commit_statement="""ALTER TABLE connct_cdc_pk2_cols10 ADD (COL_N_2 INTEGER)""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'COL_N_2', 'data_type': 'INTEGER', 'params': [], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE connct_cdc_pk2_cols10 ADD COL_N_2 INTEGER"
  )

, TestCase(
    description="Modify to varchar2 with CHAR word in parameter",
    input_table_name="CWQ_QUEUED_WORK_TYPE",
    input_commit_statement="""ALTER TABLE CWQ_QUEUED_WORK_TYPE MODIFY DESCRIPTION VARCHAR2(400 CHAR) """,
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.MODIFY, 'field_name': 'DESCRIPTION', 'data_type': 'VARCHAR2', 'params': ['400'], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE CWQ_QUEUED_WORK_TYPE MODIFY DESCRIPTION VARCHAR2(400)"
  )

, TestCase(
    description="Add varchar2 with CHAR word in parameter",
    input_table_name="CWQ_QUEUED_WORK_TYPE",
    input_commit_statement="""ALTER TABLE CWQ_QUEUED_WORK_TYPE ADD DESCRIPTION VARCHAR2(400 CHAR) """,
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'DESCRIPTION', 'data_type': 'VARCHAR2', 'params': ['400'], 'constraints': ''}
    ],
    expected_sql="ALTER TABLE CWQ_QUEUED_WORK_TYPE ADD DESCRIPTION VARCHAR2(400)"
  )

, TestCase(
    description="Add column with constraints",
    input_table_name="AAS_AGENT_RESERVATION",
    input_commit_statement="""ALTER TABLE AAS_AGENT_RESERVATION ADD ( TENANT_ID VARCHAR2(256 CHAR) DEFAULT 'default' NOT NULL )""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'TENANT_ID', 'data_type': 'VARCHAR2', 'params': ['256'], 'constraints': "DEFAULT 'default' NOT NULL"}
    ],
    expected_sql="ALTER TABLE AAS_AGENT_RESERVATION ADD TENANT_ID VARCHAR2(256) DEFAULT 'default' NOT NULL"
  )

, TestCase(
    description="Add two columns with constraints",
    input_table_name="AAS_AGENT_RESERVATION",
    input_commit_statement="""ALTER TABLE AAS_AGENT_RESERVATION ADD ( TENANT_ID VARCHAR2(256 CHAR) DEFAULT 'default' NOT NULL, TENANT_ID2 VARCHAR2(256 CHAR) DEFAULT 'default' NOT NULL )""",
    input_primary_key_fields=None,
    expected_entries=[
        {'operation': const.ADD, 'field_name': 'TENANT_ID', 'data_type': 'VARCHAR2', 'params': ['256'], 'constraints': "DEFAULT 'default' NOT NULL"},
        {'operation': const.ADD, 'field_name': 'TENANT_ID2', 'data_type': 'VARCHAR2', 'params': ['256'], 'constraints': "DEFAULT 'default' NOT NULL"}
    ],
    expected_sql="ALTER TABLE AAS_AGENT_RESERVATION ADD TENANT_ID VARCHAR2(256) DEFAULT 'default' NOT NULL, ADD TENANT_ID2 VARCHAR2(256) DEFAULT 'default' NOT NULL"
  )
]
