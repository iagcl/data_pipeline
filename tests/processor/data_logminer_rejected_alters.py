import collections

TestCase = collections.namedtuple('TestCase', "description input_table_name input_commit_statement input_primary_key_fields expected_entries expected_sql")

tests=[
 TestCase(
    description="Rejected",
    input_table_name="ALS2", 
    input_commit_statement="""ALTER TABLE ALS2 shrink space check""", 
    input_primary_key_fields=None,
    expected_entries=[],
    expected_sql=''
  )
]
