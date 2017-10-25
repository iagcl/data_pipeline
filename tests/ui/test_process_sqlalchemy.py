from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, DateTime, Numeric, Text, Sequence, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

db_string = "postgres://testuser:pa55word@localhost:5432/testdb"

db = create_engine(db_string)
base = declarative_base()

class ProcessControl(base):
    __tablename__ = 'process_control'

    id = Column(Integer, Sequence('process_control_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    profile_version = Column(Integer)
    process_code = Column(String(20))
    process_name = Column(String(30))
    source_system_code = Column(String(10))
    source_system_type = Column(String(10))
    source_region = Column(String(30))
    target_system = Column(String(30))
    target_system_type = Column(String(10))
    target_region = Column(String(30))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    status = Column(String(20))
    duration = Column(Numeric(precision=20, scale=4))
    comment = Column(String(300))
    filename = Column(String(1024))
    executor_run_id = Column(BigInteger)
    executor_status = Column(String(10))
    object_list = Column(Text)

class ProcessControlDetail(base):
    __tablename__ = 'process_control_detail'

    id = Column(Integer, Sequence('process_control_detail_id_seq'), primary_key=True)
    run_id = Column(Integer, ForeignKey('process_control.id'))
    process_code = Column(String(20))
    object_schema = Column(String(30))
    object_name = Column(String(50))
    process_starttime = Column(DateTime)
    process_endtime = Column(DateTime)
    status = Column(String(20))
    source_row_count = Column(BigInteger)
    insert_row_count = Column(BigInteger)
    update_row_count = Column(BigInteger)
    delete_row_count = Column(BigInteger)
    bad_row_count = Column(BigInteger)
    duration = Column(Numeric(precision=20, scale=4))
    delta_starttime = Column(DateTime)
    delta_endtime = Column(DateTime)
    delta_startlsn = Column(String(30))
    delta_endlsn = Column(String(30))
    error_message = Column(String(300))
    comment = Column(String(300))
    query_condition = Column(String(2048))
    
Session = sessionmaker(db)
session = Session()

base.metadata.create_all(db)

# Create Master Record
process1 = ProcessControl(profile_name="SAM", profile_version="100", process_code="process_code", process_name="process_name", source_system_code="src_code", source_system_type="src_type", source_region="source_region", target_system="target_system", target_system_type="trg_type", target_region="target_region", process_starttime="2017-01-01 00:00:00", process_endtime="2017-01-01 00:00:00", min_lsn="min_lsn", max_lsn="max_lsn", status="status", duration="100", comment="comment", filename="filename",  executor_run_id="1000",  executor_status="ex_status", object_list="object_list")

session.add(process1)
session.commit()

# Create Detail records
detail1 = ProcessControlDetail(run_id="1", process_code="process_code", object_schema="object_schema", object_name="object_name", process_starttime="2017-01-01 00:00:00", process_endtime="2017-01-01 00:00:00", status="initiated", source_row_count="100", insert_row_count="200", update_row_count="300", delete_row_count="400", bad_row_count="0",  duration="100", delta_starttime="2017-01-01 00:00:00", delta_endtime="2017-01-01 00:00:00", delta_startlsn="delta_startlsn", delta_endlsn="delta_endlsn", error_message="error_message", comment="comment", query_condition="query_condition")

session.add(detail1)
session.commit()

detail2 = ProcessControlDetail(run_id="1", process_code="process_code", object_schema="object_schema", object_name="object_name", process_starttime="2017-01-01 00:00:00", process_endtime="2017-01-01 00:00:00", status="initiated", source_row_count="100", insert_row_count="200", update_row_count="300", delete_row_count="400", bad_row_count="0",  duration="100", delta_starttime="2017-01-01 00:00:00", delta_endtime="2017-01-01 00:00:00", delta_startlsn="delta_startlsn", delta_endlsn="delta_endlsn", error_message="error_message", comment="comment", query_condition="query_condition")

session.add(detail2)
session.commit()

# Read Master
processes = session.query(ProcessControl)
for process in processes:
    print(process.process_code)


print "details:"

# Read Detail
processes = session.query(ProcessControlDetail)
for process in processes:
    print(process.process_code)
    
# Update
#process1.profile_version = "profile_version_2"
#session.commit()

# Delete
#session.delete(process1)
#session.commit()

