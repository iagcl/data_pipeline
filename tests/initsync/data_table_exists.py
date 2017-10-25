import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "input_dbtype expect_sql_call")

ORACLE_TABLE_EXISTS_SQL = """
        SELECT COUNT(*) AS CNT
        FROM ALL_OBJECTS
        WHERE OBJECT_TYPE IN ('TABLE','VIEW')
          AND OWNER       = UPPER('myschema')
          AND OBJECT_NAME = UPPER('mytable')"""


POSTGRES_TABLE_EXISTS_SQL = """
        SELECT EXISTS (
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'myschema'
              AND table_name   = 'mytable'
        )"""


MSSQL_TABLE_EXISTS_SQL = """
        IF EXISTS (
          SELECT * FROM INFORMATION_SCHEMA.TABLES
          WHERE LOWER([table_schema]) = LOWER('myschema')
          AND   LOWER([table_name]) = LOWER('mytable')
        )
          BEGIN
            SELECT 1
          END
        ELSE
          BEGIN
            SELECT 0
          END"""

tests=[

    TestCase(
        input_dbtype=const.GREENPLUM,
        expect_sql_call=POSTGRES_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.POSTGRES,
        expect_sql_call=POSTGRES_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.ORACLE,
        expect_sql_call=ORACLE_TABLE_EXISTS_SQL,
    ),

    TestCase(
        input_dbtype=const.MSSQL,
        expect_sql_call=MSSQL_TABLE_EXISTS_SQL,
    ),
]
