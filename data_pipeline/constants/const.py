###############################################################################
# Module:    const
# Purpose:   Collection of global constants
#
# Notes:
#
###############################################################################

CDCAPPLY = 'CDCApply'
CDCEXTRACT = 'CDCExtract'
INITSYNC = 'InitSync'
INITSYNCEXTRACT = 'InitSyncExtract'
INITSYNCAPPLY = 'InitSyncApply'
INITSYNCTRUNC = 'InitSyncTrunc'
CSV = 'csv'
TEXT = 'text'
SRC = 'src'
DEST = 'dest'
FILE = 'file'
ORACLE = 'oracle'
MSSQL = 'mssql'
POSTGRES = 'postgres'
GREENPLUM = 'greenplum'
JSON = 'json'

# File extensions
GZ = 'gz'
BZ2 = 'bz2'

DELIMITER = '~'
FIELD_DELIMITER = '\x02'
KEYFIELD_DELIMITER = ','
NO_KEYFIELD_STR = 'NOPK'
FOUND = 'Y'
EXTRACTOBJECTNAME = 'LogExtract'

# Logging constants
INFO_HANDLER = "info_file_handler"
ERROR_HANDLER = "error_file_handler"
CACHED_HANDLERS = [INFO_HANDLER, ERROR_HANDLER]

# Audit constants
PROCESS_CONTROL_TABLE = 'process_control'
PROCESS_CONTROL_DETAIL_TABLE = 'process_control_detail'
SOURCE_SYSTEM_PROFILE_TABLE = 'source_system_profile'
MAX_COMMENT_LENGTH = 300
MIN_COMMIT_POINT = 50
MAX_AUDIT_ATTEMPTS = 5

# Characters
EMPTY_STRING = ''
EQUALS = '='
SPACE = ' '
COMMA = ','
DOT = '.'
COMMASPACE = ', '
LEFT_BRACKET = '('
RIGHT_BRACKET = ')'
DOUBLE_QUOTE = '"'
SINGLE_QUOTE = "'"
FORWARDSLASH = "/"
BACKSLASH = "\\"
PERCENT = "%"
UNDERSCORE = '_'
CHAR_CR = '\r'
CHAR_LF = '\n'
CHAR_CRLF = '\r\n'
CHAR_NULL = '\x00'
CHAR_MIN_ASCII = chr(0)
CHAR_MAX_ASCII = chr(127)

# Attributes
ALTER_ENTRY = 'alter_entry'
CREATE_ENTRY = 'create_entry'
OPERATION = 'operation'
FIELD_NAME = 'field_name'
DATA_TYPE = 'data_type'
PARAMS = 'params'
CONSTRAINTS = 'constraints'

# Operations
ADD = 'ADD'
MODIFY = 'MODIFY'
SUPPORTED_OPERATIONS = [ADD, MODIFY]

# Process Control statuses
IN_PROGRESS = 'IN_PROGRESS'
SUCCESS = 'SUCCESS'
ERROR = 'ERROR'
WARNING = 'WARNING'
KILLED = 'KILLED'
SKIPPED = 'SKIPPED'
COMMITTED = 'COMMITTED'
UNCOMMITTED = 'UNCOMMITTED'
ROLLBACK = 'ROLLBACK'

# SQL processing
DEFAULT_ARRAYSIZE = 1000
INSERT = 'INSERT'
UPDATE = 'UPDATE'
DELETE = 'DELETE'
DDL = 'DDL'
CREATE = 'CREATE'
CREATE_TABLE = 'CREATE TABLE'
ALTER = 'ALTER'
ALTER_TABLE = 'ALTER TABLE'
TYPE = 'TYPE'
SET = 'set'
WHERE = 'where'
IS = 'IS'
NULL = 'NULL'
IS_NULL = 'IS NULL'
CONSTRAINT = 'CONSTRAINT'
BEGIN = "BEGIN"
COMMIT = "COMMIT"

MSSQL_DELETE_ACTION = '1'
MSSQL_INSERT_ACTION = '2'
MSSQL_UPDATE_ACTION = '4'
MSSQL_DEFAULT_SCHEMA = 'dbo'

ORACLE_SET_NULL = "= NULL"
ORACLE_EMPTY_CLOB = "EMPTY_CLOB()"
ORACLE_EMPTY_BLOB = "EMPTY_BLOB()"

# Record types
START_OF_BATCH = 'SOB'
END_OF_BATCH = 'EOB'
DATA = 'DATA'
KILL = 'KILL'

# Data types
DICT = 'dict'

# Special value handline
NEW_PARSING_STATE = "new_parsing_state"
ASSIGN_VALUE = "assign_value"

# Oracle Extractor
ONLINE_DICT = 'online'
REDOLOG_DICT = 'redolog'
OPERATION_FIELD = 'OPERATION'
STATEMENT_ID_FIELD = 'STATEMENT_ID'
CSCN_FIELD = 'CSCN'
SOURCE_TIMESTAMP_FIELD = 'SOURCE_TIMESTAMP'
TABLE_NAME_FIELD = 'TABLE_NAME'
CSF_FLAG_FIELD = 'CSF_FLAG'
SEG_OWNER_FIELD = 'SEG_OWNER'
SQLREDOSTMT_FIELD = 'SQLREDOSTMT'
ROLLBACK_FLAG_FIELD = 'ROLLBACK_FLAG'
COUNT_FIELD = 'CNT'

sql = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
ALTER_NLS_DATE_FORMAT_COMMAND = sql

sql = "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
ALTER_NLS_TIMESTAMP_FORMAT_COMMAND = sql

# Initsync constants
HEARTBEAT_PERIOD = 1000
SOURCE = "source"
TARGET = "target"

# Data mapping config constants
DEFAULT = 'default'
RULES = 'rules'
TARGET = 'target'
PRECISION = 'precision'
PRECISION_START = 'start'
PRECISION_END = 'end'
PRECISION_PARAM_INDEX = 0
SCALE_PARAM_INDEX = 1
KEEP_PARAMS = 'keep_params'
DEFAULT_DATATYPE = 'VARCHAR'

# Logging constants
HANDLERS = 'handlers'
CLASS = 'class'
FILE_HANDLER = 'FileHandler'
FILENAME = 'filename'

# Kafka constants
KAFKA_DEFAULT_POLL_TIMEOUT = 10
KAFKA_MAX_COMMIT_RETRIES = 10
KAFKA_MAX_BUFFERED_MS = 100
KAFKA_MAX_BUFFERED_MSG = 10000000
KAFKA_AUTO_OFFSET_RESET = 'earliest'
KAFKA_STATS_INTERVAL_MS = 10

# Email constants
MAX_SUBJECT_LENGTH = 80

# ASCII char codes
ASCII_NULL = 0
ASCII_STARTOFTEXT = 2
ASCII_NEWLINE = 10
ASCII_CARRIAGERETURN = 13
ASCII_GROUPSEPARATOR = 29
ASCII_RECORDSEPARATOR = 30

# Metadata constants
METADATA_INSERT_TS_COL = 'insert_timestamp_column'
METADATA_UPDATE_TS_COL = 'update_timestamp_column'
METADATA_CURRENT_TIME_SQL = '(SELECT CURRENT_TIMESTAMP)'

# SSL Modes
SSLMODE_DISABLE = "disable"
SSLMODE_ALLOW = "allow"
SSLMODE_PREFER = "prefer"
SSLMODE_REQUIRE = "require"
SSLMODE_VERIFY_CA = "verify-ca"
SSLMODE_VERIFY_FULL = "verify-full"

# Replace special chars in column names with this character to
# make sure the bulk write is able to deal with the column name.
# For example: On source, there could be a column named "/MSG/RAJJ"
# and target should then have "_msg_rajj". Note here that another
# convention used is to lowercase the column name on target.
# Note: this assumes that the target db has these columns setup
COLUMN_NAME_SPECIAL_CHARS = [FORWARDSLASH, BACKSLASH]
SPECIAL_CHAR_REPLACEMENT = UNDERSCORE

# Supported Oracle data types
ORACLE_SUPPORTED_DATATYPES = [
    'VARCHAR',
    'VARCHAR2',
    'NVARCHAR2',
    'NUMBER',
    'INTEGER',
    'INT',
    'SMALLINT',
    'FLOAT',
    'REAL',
    'LONG',
    'DATE',
    'BINARY_FLOAT',
    'BINARY_DOUBLE',
    'TIMESTAMP',
    'TIMESTAMP2',
    'TIME',
    'INTERVAL',
    'RAW',
    'ROWID',
    'UROWID',
    'CHARACTER',
    'CHAR',
    'BYTE',
    'NCHAR',
    'CLOB',
    'NCLOB'
]
