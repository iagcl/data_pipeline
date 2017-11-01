# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# 
import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statement input_primary_key_fields expected_entries expected_sql")

tests=[
  TestCase(
    description="Two fields, no constraints",
    input_table_name="als", 
    input_commit_statement="""create table als (id integer , description varchar2(100))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': None}
        , {'field_name': 'description', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': None}
    ],
    expected_sql="CREATE TABLE als (id INTEGER, description VARCHAR2(100))"
  )

,  TestCase(
    description="Two fields, last field with comma params, no constraints",
    input_table_name="als", 
    input_commit_statement="""create table als (id integer , description number(1, 2))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': None}
        , {'field_name': 'description', 'data_type': 'NUMBER', 'params': ['1', '2'], 'constraints': None}
    ],
    expected_sql="CREATE TABLE als (id INTEGER, description NUMBER(1, 2))"
  )

,  TestCase(
    description="Two fields, with primary key constraints",
    input_table_name="als", 
    input_commit_statement="""create table als (id integer NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description varchar2(100))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': 'NOT NULL CONSTRAINT EMP_PK PRIMARY KEY'}
        , {'field_name': 'description', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': None}
    ],
    expected_sql="CREATE TABLE als (id INTEGER NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description VARCHAR2(100))"
  )

,  TestCase(
    description="Two fields, with foreign key constraints",
    input_table_name="als", 
    input_commit_statement="""create table als (id integer , description varchar2(100), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': None}
        , {'field_name': 'description', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': None}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER)'}
    ],
    expected_sql="CREATE TABLE als (id INTEGER, description VARCHAR2(100), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))", 
  )


,  TestCase(
    description="Two fields, with primary and foreign key constraints",
    input_table_name="als", 
    input_commit_statement="""create table als (id INTEGER NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description VARCHAR2(100), CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': 'NOT NULL CONSTRAINT EMP_PK PRIMARY KEY'}
        , {'field_name': 'description', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': None}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER)'}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER)'}
    ],
    expected_sql="CREATE TABLE als (id INTEGER NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description VARCHAR2(100), CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))", 
  )

,  TestCase(
    description="Two fields, with primary and foreign key constraints and new lines",
    input_table_name="als", 
    input_commit_statement="""create table als (id INTEGER NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description VARCHAR2(100), CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'id', 'data_type': 'INTEGER', 'params': [], 'constraints': 'NOT NULL CONSTRAINT EMP_PK PRIMARY KEY'}
        , {'field_name': 'description', 'data_type': 'VARCHAR2', 'params': ['100'], 'constraints': None}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER)'}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER)'}
    ],
    expected_sql="CREATE TABLE als (id INTEGER NOT NULL CONSTRAINT EMP_PK PRIMARY KEY, description VARCHAR2(100), CONSTRAINT FLTAVAIL_PK PRIMARY KEY (ID, SEGMENT_NUMBER), CONSTRAINT FLTS_FK FOREIGN KEY (ID, SEGMENT_NUMBER) REFERENCES Flights (ID, SEGMENT_NUMBER))", 
  )

, TestCase(
    description="Real-world example",
    input_table_name="als", 
    input_commit_statement="""                 CREATE TABLE sys.CONNCT_CDC_PK5_COLS10  ( compndPk_1 NUMBER(15,5) NOT NULL, compndPk_2 NUMBER(15,5) NOT NULL, compndPk_3 NUMBER(15,5) NOT NULL, compndPk_4 NUMBER(15,5) NOT NULL, compndPk_5 NUMBER(15,5) NOT NULL, col_ts_0      TIMESTAMP(6), col_v_1 \tVARCHAR2(25) NULL, col_v_2 \tVARCHAR2(25) NULL, col_v_3 \tVARCHAR2(25) NULL, col_v_4 \tVARCHAR2(25) NULL, col_n_5 \tNUMBER(15,5) NULL, col_n_6 \tNUMBER(15,5) NULL, col_n_7 \tNUMBER(15,5) NULL, col_n_8 \tNUMBER(15,5) NULL, col_n_9 \tNUMBER(15,5) NULL, col_nas_9 \tVARCHAR2(4000) NULL,CONSTRAINT cnst_CONNCT_CDC_PK5_COLS10 PRIMARY KEY( compndPk_1, compndPk_2, compndPk_3, compndPk_4, compndPk_5))""", 
    input_primary_key_fields=None,
    expected_entries=[
        {'field_name': 'compndPk_1', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NOT NULL'}
        , {'field_name': 'compndPk_2', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NOT NULL'}
        , {'field_name': 'compndPk_3', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NOT NULL'}
        , {'field_name': 'compndPk_4', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NOT NULL'}
        , {'field_name': 'compndPk_5', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NOT NULL'}
        , {'field_name': 'col_ts_0', 'data_type': 'TIMESTAMP', 'params': ['6'], 'constraints': None}
        , {'field_name': 'col_v_1', 'data_type': 'VARCHAR2', 'params': ['25'], 'constraints': 'NULL'}
        , {'field_name': 'col_v_2', 'data_type': 'VARCHAR2', 'params': ['25'], 'constraints': 'NULL'}
        , {'field_name': 'col_v_3', 'data_type': 'VARCHAR2', 'params': ['25'], 'constraints': 'NULL'}
        , {'field_name': 'col_v_4', 'data_type': 'VARCHAR2', 'params': ['25'], 'constraints': 'NULL'}
        , {'field_name': 'col_n_5', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NULL'}
        , {'field_name': 'col_n_6', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NULL'}
        , {'field_name': 'col_n_7', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NULL'}
        , {'field_name': 'col_n_8', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NULL'}
        , {'field_name': 'col_n_9', 'data_type': 'NUMBER', 'params': ['15', '5'], 'constraints': 'NULL'}
        , {'field_name': 'col_nas_9', 'data_type': 'VARCHAR2', 'params': ['4000'], 'constraints': 'NULL'}
        , {'field_name': None, 'data_type': None, 'params': [], 'constraints': 'CONSTRAINT cnst_CONNCT_CDC_PK5_COLS10 PRIMARY KEY( compndPk_1, compndPk_2, compndPk_3, compndPk_4, compndPk_5)'}
    ],
    expected_sql="CREATE TABLE als (compndPk_1 NUMBER(15, 5) NOT NULL, compndPk_2 NUMBER(15, 5) NOT NULL, compndPk_3 NUMBER(15, 5) NOT NULL, compndPk_4 NUMBER(15, 5) NOT NULL, compndPk_5 NUMBER(15, 5) NOT NULL, col_ts_0 TIMESTAMP(6), col_v_1 VARCHAR2(25) NULL, col_v_2 VARCHAR2(25) NULL, col_v_3 VARCHAR2(25) NULL, col_v_4 VARCHAR2(25) NULL, col_n_5 NUMBER(15, 5) NULL, col_n_6 NUMBER(15, 5) NULL, col_n_7 NUMBER(15, 5) NULL, col_n_8 NUMBER(15, 5) NULL, col_n_9 NUMBER(15, 5) NULL, col_nas_9 VARCHAR2(4000) NULL, CONSTRAINT cnst_CONNCT_CDC_PK5_COLS10 PRIMARY KEY( compndPk_1, compndPk_2, compndPk_3, compndPk_4, compndPk_5))"
  )


]
