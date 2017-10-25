import importlib
import pytest
import json
import data_pipeline.constants.const as const
import data_pipeline.processor.factory as processor_factory
import data_pipeline.processor.oracle_cdc_processor as oracle_cdc_processor

from pytest_mock import mocker
from data_pipeline.stream.oracle_message import OracleMessage
from data_pipeline.processor.exceptions import UnsupportedSqlError


def build_field_indices(fields):
    lookup = {}
    i = 0
    for f in fields:
        lookup[f] = i
        i += 1

    return lookup


def load_tests(name):
    # Load module which contains test data
    tests_module = importlib.import_module(name)
    # Tests are to be found in the variable `tests` of the module
    for test in tests_module.tests:
        yield test


def pytest_generate_tests(metafunc):
    """ This allows us to load tests from external files by
    parametrizing tests with each test case found in a data_X
    file """
    for fixture in metafunc.fixturenames:
        if fixture.startswith('data_'):
            # Load associated test data
            current_package = __name__.rpartition('.')[0]
            tests = load_tests("{}.{}".format(current_package, fixture))
            metafunc.parametrize(fixture, tests)


@pytest.mark.parametrize("operation_code, commit_statement, expected_type", [
    (const.INSERT, 'insert into table() values ()', 'InsertStatement'),
    (const.UPDATE, 'update table set where ', 'UpdateStatement'),
    (const.DELETE, 'delete from table where ', 'DeleteStatement'),
    (const.DDL, "COMMENT ON COLUMN EVA_MIGRATION_ROLLBACK_DATA.EXPORT_LOCALE IS 'To store Entity Minor Version of an object.'", 'NoneType'),
])
def test_process_returns_correct_instance_type(operation_code, commit_statement, expected_type, mocker):

    oracle_message = OracleMessage()
    oracle_message.operation_code = operation_code
    oracle_message.commit_statement = commit_statement
    oracle_message.primary_key_fields = ""

    processor = processor_factory.build(const.ORACLE)
    statement = processor.process(oracle_message)

    assert type(statement).__name__ == expected_type


@pytest.mark.parametrize("operation_code", [
    (const.INSERT),
    (const.UPDATE),
    (const.DELETE),
    (const.EMPTY_STRING),
])
def test_process_empty_message(operation_code, mocker):
    oracle_message = OracleMessage()
    oracle_message.operation_code = operation_code

    processor = processor_factory.build(const.ORACLE)
    statement = processor.process(oracle_message)
    assert statement is None

def execute_statement_parsing_test(operation, data, mocker):
    print("Running test: '{}'".format(data.description))
    
    oracle_message = OracleMessage()
    oracle_message.operation_code = operation
    oracle_message.table_name = data.input_table_name
    oracle_message.commit_statement = data.input_commit_statement
    oracle_message.primary_key_fields = data.input_primary_key_fields

    processor = processor_factory.build(const.ORACLE)
    output = processor.process(oracle_message)
    return output

def test_process_logminer_insert_statements(data_logminer_inserts, mocker):
    statement = execute_statement_parsing_test(const.INSERT, data_logminer_inserts, mocker)

    field_lookup = build_field_indices(data_logminer_inserts.expected_fields)
    fields = statement.get_fields()
    i = 0
    print("statement={}".format(statement))
    for field in fields:
        assert field in data_logminer_inserts.expected_fields
        assert statement.get_value(field) == data_logminer_inserts.expected_values[field_lookup[field]]
        i += 1

def test_process_logminer_update_statements(data_logminer_updates, mocker):
    statement = execute_statement_parsing_test(const.UPDATE, data_logminer_updates, mocker)
    if statement is None:
        assert len(data_logminer_updates.expected_entries) == 0
        assert len(data_logminer_updates.expected_sql) == 0
    else:
        assert statement.set_values == data_logminer_updates.expected_set_values
        assert str(statement) == data_logminer_updates.expected_sql

def test_process_logminer_delete_statements(data_logminer_deletes, mocker):
    statement = execute_statement_parsing_test(const.DELETE, data_logminer_deletes, mocker)
    if statement is None:
        assert len(data_logminer_deletes.expected_sql) == 0
    else:
        assert str(statement) == data_logminer_deletes.expected_sql

def test_process_unsupported_operation(data_logminer_rejected_alters):
    with pytest.raises(UnsupportedSqlError):
        statement = execute_statement_parsing_test(const.DDL, data_logminer_rejected_alters, mocker)

def test_process_logminer_alter_statements(data_logminer_alters, mocker):
    statement = execute_statement_parsing_test(const.DDL, data_logminer_alters, mocker)
    if statement is None:
        assert len(data_logminer_alters.expected_entries) == 0
        assert len(data_logminer_alters.expected_sql) == 0
    else:
        assert statement.entries == data_logminer_alters.expected_entries
        assert str(statement) == data_logminer_alters.expected_sql

def test_process_logminer_create_statements(data_logminer_creates, mocker):
    statement = execute_statement_parsing_test(const.DDL, data_logminer_creates, mocker)
    if statement is None:
        assert len(data_logminer_creates.expected_entries) == 0
        assert len(data_logminer_creates.expected_sql) == 0
    else:
        assert statement.entries == data_logminer_creates.expected_entries
        assert str(statement) == data_logminer_creates.expected_sql
