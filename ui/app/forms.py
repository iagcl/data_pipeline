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
from flask_wtf import Form
from wtforms import TextField, IntegerField, TextAreaField, SubmitField, RadioField, SelectField, StringField, PasswordField, BooleanField, DateField, DateTimeField
from wtforms.ext.sqlalchemy.fields import QuerySelectField

from wtforms.validators import DataRequired, Length

import datetime

from wtforms import validators, ValidationError


class ProfileForm(Form):
    profile_name = StringField('Profile Name :', validators=[DataRequired(), Length(min=1, max=20)])
    profile_version = IntegerField('Profile Version :', validators=[DataRequired()])
    source_system_code = StringField('Source System :', validators=[DataRequired(), Length(min=1, max=10)])
    source_region = StringField('Source Region :', validators=[Length(max=30)])  
    target_region = StringField('Target Region :', validators=[Length(max=30)])
    object_name = StringField('Object Name :', validators=[DataRequired(), Length(min=1, max=50)])
    active_ind = SelectField('Is Active :',choices=[('Y','Active'),('N','Not Active')],default='Y') 
    min_lsn = StringField('Min LSN :',  validators=[Length(max=30)])  
    max_lsn = StringField('Max LSN :', validators=[Length(max=30)])  
    notes = StringField('Notes :',  validators=[Length(max=4000)])  
    
class ProfileItemForm(Form):
    object_seq = IntegerField('Object Sequence :', validators=[DataRequired()])
    object_name = StringField('Object Name :',  validators=[DataRequired(), Length(min=1, max=50)])  
    active_ind = SelectField('Is Active :',choices=[('Y','Active'),('N','Not Active')],default='Y') 
    min_lsn = StringField('Min LSN :',  validators=[Length(max=30)])  
    max_lsn = StringField('Max LSN :', validators=[Length(max=30)])  
    last_status = StringField('Last Status :', validators=[Length(max=20)])  
    last_process_code = StringField('Last Process Code :', validators=[Length(max=20)])  
    notes = StringField('Notes :',  validators=[Length(max=4000)]) 
    query_condition = StringField('Query Condition :',  validators=[Length(max=4000)])      

      
class ProfileHeaderForm(Form):
    profile_name = StringField('Profile Name :', validators=[DataRequired(), Length(min=1, max=20)])
    version = IntegerField('Profile Version :', validators=[DataRequired()])
    source_system = StringField('Source System :', validators=[DataRequired(), Length(min=1, max=30)])
    source_system_code = StringField('Source System Code :', validators=[DataRequired(), Length(min=1, max=30)])
    source_database_type = SelectField('Source Database Type :',choices=[('Oracle','Oracle'),('MSSQL','MSSQL'),('Postgres','Postgres'),('DB2','DB2'),('Kafka','Kafka')],default='Oracle')     
    source_connection = QuerySelectField('Source Connection :', get_pk=lambda a: a.connection_name, get_label=lambda a: a.connection_name, allow_blank=False)  
    target_system = StringField('Target System :', validators=[DataRequired(), Length(max=30)])
    target_system_code = StringField('Target System Code :', validators=[Length(max=30)])
    target_database_type = SelectField('Target Database Type :',choices=[('Oracle','Oracle'),('MSSQL','MSSQL'),('Postgres','Postgres'),('DB2','DB2'),('GreenPlum','GreenPlum'),('Kafka','Kafka')],default='Oracle')     
    target_connection = StringField('Target Connection :', validators=[Length(max=20)])  
    active_ind = SelectField('Is Active :',choices=[('Y','Active'),('N','Not Active')],default='Y')
    server_path = StringField('Server Run Script Path :', validators=[Length(max=4000)])    
    description = StringField('Description :',  validators=[Length(max=1000)])
    
class ConnectionForm(Form):
    connection_name = StringField('Connection Name :',  validators=[DataRequired(), Length(min=1, max=20)])  
    connection_category = SelectField('Connection Category :',choices=[('Source','Source'),('Target','Target')],default='Source') 
    database_type = SelectField('Database Type :',choices=[('Oracle','Oracle'),('MSSQL','MSSQL'),('Postgres','Postgres'),('GreenPlum','GreenPlum'),('DB2','DB2')],default='Oracle') 
    hostname = StringField('Server Name:',  validators=[DataRequired(), Length(min=1, max=100)]) 
    portnumber = IntegerField('Port Number:', validators=[DataRequired()])
    username = StringField('User Name:',  validators=[DataRequired(), Length(min=1, max=50)])
    password = StringField('Password:',  validators=[DataRequired(), Length(min=1, max=50)])
    database_name = StringField('SID or Database:',  validators=[DataRequired(), Length(min=1, max=50)])
    notes = StringField('Notes :',  validators=[Length(max=200)])  
    
class UserForm(Form):
    username = StringField('User Name :',  validators=[DataRequired(), Length(min=1, max=20)])  
    firstname = StringField('First Name :',  validators=[DataRequired(), Length(min=1, max=20)]) 
    lastname = StringField('Last Name :',  validators=[DataRequired(), Length(min=1, max=20)])     
    email = StringField('Email :',  validators=[DataRequired(), Length(min=4, max=50)])     
    role = SelectField('Role :',choices=[('Admin','Admin'),('Manager','Manager'),('User','User')],default='User') 
    password = StringField('Password :', validators=[DataRequired(), Length(min=4, max=10)])

class LoginForm(Form):
    username = StringField('User Name :',  validators=[DataRequired(), Length(min=1, max=20)])
    password = PasswordField('Password :', validators=[DataRequired(), Length(min=4, max=10)])
    
class ParameterForm(Form):
    parameter_name = StringField('Parameter Name :',  validators=[DataRequired(), Length(min=1, max=30)]) 
    parameter_type = SelectField('Parameter Type :',choices=[('VARCHAR','VARCHAR'),('NUMERIC','NUMERIC'),('DATE','DATE')],default='VARCHAR')     
    parameter_value = StringField('Parameter Value :',  validators=[DataRequired(), Length(min=1, max=300)]) 
