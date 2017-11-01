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
##################################################################################
# Module:    models.py
# Purpose:   Flask SQLAlchemy ORM objects
#
# Notes:
#
##################################################################################

from sqlalchemy import Boolean, Column, String, Integer, Numeric, DateTime, BigInteger, Text, Sequence, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class ProcessControl(Base):
    __tablename__ = 'process_control'

    id = Column(Integer, Sequence('process_control_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    profile_version = Column(Integer)
    process_code = Column(String(20))
    process_name = Column(String(30))
    source_system_code = Column(String(30))
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
    dml_count = Column(BigInteger)
    ddl_count = Column(BigInteger)
    other_count = Column(BigInteger)
    total_count = Column(BigInteger)               
    comment = Column(String(4000))
    filename = Column(String(1024))
    infolog = Column(String(1024))
    errorlog = Column(String(1024))
    executor_run_id = Column(BigInteger)
    executor_status = Column(String(10))
    executor_logs = Column(Integer)
    archive_logs = Column(Integer)          
    object_list = Column(Text)

    def __repr__(self):
        return '<ProcessControl %r>' % (self.profile_name)
        
class ProcessControlDetail(Base):
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
    alter_count = Column(BigInteger)
    create_count = Column(BigInteger)
    delta_starttime = Column(DateTime)
    delta_endtime = Column(DateTime)
    delta_startlsn = Column(String(30))
    delta_endlsn = Column(String(30))
    error_message = Column(String(300))
    comment = Column(String(300))
    filename = Column(String(4000))
    linked_run_id = Column(BigInteger)
    query_condition = Column(String(4000))
    errorlog = Column(String(1024))
    infolog = Column(String(1024))
    duration = Column(Numeric(precision=20, scale=4))
    
    def __repr__(self):
        return '<ProcessControlDetail %r>' % (self.id)
        
class SourceSystemProfile(Base):
    __tablename__ = 'source_system_profile'
 
    id = Column(Integer, Sequence('source_system_profile_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    version = Column(Integer)
    source_system_code = Column(String(30))
    source_region = Column(String(30))
    target_region = Column(String(30))
    object_name = Column(String(50))
    object_seq = Column(BigInteger)
    min_lsn = Column(String(30))
    max_lsn = Column(String(30))
    active_ind = Column(String(1))
    history_ind = Column(String(1))
    applied_ind = Column(String(1))
    delta_ind = Column(String(1))
    last_run_id = Column(Integer)
    last_process_code = Column(String(20))
    last_status = Column(String(20))
    last_updated = Column(DateTime)
    last_applied = Column(DateTime)
    last_history_update = Column(DateTime)
    notes = Column(String(4000))
    query_condition = Column(String(4000))
 
    def __repr__(self):
        return '<SourceSystemProfile %r>' % (self.id) 


class Connections(Base):
    __tablename__ = 'connections'
 
    id = Column(Integer, Sequence('connections_id_seq'), primary_key=True)
    connection_name = Column(String(30))
    connection_category = Column(String(10))
    database_type = Column(String(10))
    hostname = Column(String(100))
    portnumber = Column(Integer)
    username = Column(String(50))
    password = Column(String(50))
    database_name = Column(String(50))    
    created_by = Column(String(50))
    created_date = Column(DateTime)
    updated_by = Column(String(50))
    updated_date = Column(DateTime)
    notes = Column(String(200))
 
    def __repr__(self):
        return '<Connections %r>' % (self.connection_name) 

class ProcessParameters(Base):
    __tablename__ = 'process_parameters'
 
    id = Column(Integer, Sequence('process_parameters_id_seq'), primary_key=True)
    parameter_name = Column(String(80))
    parameter_type = Column(String(10))
    parameter_value = Column(String(300))
 
    def __repr__(self):
        return '<ProcessParameters %r>' % (self.name)   

class ReferenceData(Base):
    __tablename__ = 'reference_data'
 
    id = Column(Integer, Sequence('reference_data_id_seq'), primary_key=True)
    domain = Column(String(50))
    code = Column(String(30))
    description = Column(String(1000))
    active_ind = Column(String(1))
    order_seq = Column(Integer)
    
    def __repr__(self):
        return '<ReferenceData %r>' % (self.id)   

        
class User(Base):
    __tablename__ = "users"
    id = Column('user_id',Integer , primary_key=True)
    username = Column('username', String(20), unique=True , index=True)
    firstname = Column('firstname', String(20))
    lastname = Column('lastname', String(20))
    password = Column('password' , String(10))
    email = Column('email',String(50),unique=True , index=True)
    role = Column('role' , String(10))
    registered_on = Column('registered_on' , DateTime)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return unicode(self.id)
        
    def get_role(self):
        return self.role
        
    def __repr__(self):
        return '<User %r>' % (self.username)
        
        
class Profile(Base):
    __tablename__ = 'profile'
 
    id = Column(Integer, Sequence('profile_id_seq'), primary_key=True)
    profile_name = Column(String(20))
    version = Column(Integer)
    source_system = Column(String(30))
    source_system_code = Column(String(30))
    source_database_type = Column(String(30))
    source_connection = Column(String(30))
    target_system = Column(String(30))
    target_system_code = Column(String(30))
    target_database_type = Column(String(30))
    target_connection = Column(String(30))
    description = Column(String(1000))
    active_ind = Column(String(1))    
    server_path = Column(String(4000))
    
    
    def __repr__(self):
        return '<Profile %r>' % (self.profile_name)  
