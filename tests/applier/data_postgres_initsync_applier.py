import collections
import data_pipeline.constants.const as const

TestCase = collections.namedtuple('TestCase', "description input_table_name input_payloads input_record_types input_record_counts expect_copy_called_times expect_commit_called_times expect_batch_committed")

tests=[
  TestCase(
    description="Single record, no end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH, const.DATA],
    input_payloads=['', "1,albert"],
    input_record_counts=[0, 0],
    expect_batch_committed=[False, False],
    expect_copy_called_times=0,
    expect_commit_called_times=0
  )

, TestCase(
    description="Start of batch record only",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH],
    input_payloads=[''],
    input_record_counts=[0],
    expect_batch_committed=[False],
    expect_copy_called_times=0,
    expect_commit_called_times=0
  )

, TestCase(
    description="Single record with end of batch",
    input_table_name="CONNCT_CDC_PK5_COLS10", 
    input_record_types=[const.START_OF_BATCH, const.DATA, const.END_OF_BATCH],
    input_payloads=['',
                    '1,albert',
                    ''
    ],
    input_record_counts=[0, 0, 1],
    expect_batch_committed=[False, False, True],
    expect_copy_called_times=1,
    expect_commit_called_times=1
  )


]
