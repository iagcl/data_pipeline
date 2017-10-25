import collections
import data_pipeline.constants.const as const
import data_postgres_cdc_applier_batch_state

TARGETSCHEMA = 'ctl'

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statements input_record_types input_operation_codes input_primary_key_fields expect_error_message")

tests = data_postgres_cdc_applier_batch_state.tests
