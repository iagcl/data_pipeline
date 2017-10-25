##################################################################################
# Module:    app.py
# Purpose:   Flask main UI logic
#
# Notes:
#
##################################################################################

from flask import Flask, render_template, request, g, flash, url_for, redirect
from sqlalchemy import create_engine,func
from sqlalchemy.orm import sessionmaker
from flask_login import LoginManager, login_user , logout_user , current_user , login_required
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

import time
import os
import sys
import subprocess


import ui.utils.args as args
import data_pipeline.utils.dbuser as dbuser

import data_pipeline.db.factory as db_factory
import data_pipeline.db.connection_details as db_connection
import data_pipeline.constants.const as const

from ui.app.models import Base, ProcessControl, ProcessControlDetail, SourceSystemProfile, User, Connections, ProcessParameters, Profile

from ui.app.forms import ProfileForm, ProfileItemForm, ConnectionForm, UserForm, LoginForm, ParameterForm, ProfileHeaderForm 

# Globals
argv = args.get_program_args()

# Audit DB details
db_string = "postgres://{conn_str}".format(conn_str=argv.audituser)
db = create_engine(db_string)  
Session = sessionmaker(db) 
session = Session()  

# Create Flask App
app = Flask(__name__)

# initialise Scheduler

scheduler = BackgroundScheduler()
url = 'sqlite:///example.sqlite'
scheduler.add_jobstore('sqlalchemy', url=db_string)

# Initialise LoginManager
login_manager = LoginManager()
login_manager.init_app(app)

@login_manager.user_loader
# user_loader callback function loads the user from the database
def load_user(id):
    return session.query(User).get(int(id))

@app.before_request
def before_request():
    g.user = current_user

@app.route("/login", methods=['GET','POST'])
def login():
   
    # initialize the form
    form = LoginForm(request.form)
    
    if request.method == 'POST' and form.validate_on_submit():
       
        username = request.form['username']
        password = request.form['password']
        registered_user = session.query(User).filter_by(username=username,password=password).first()
        if registered_user is None:
            flash('Username or Password is invalid' , 'error')
            return redirect(url_for('login'))
            
        login_user(registered_user)
        next =  request.args.get('next')
 
        return redirect(next or url_for('main'))
       
    return render_template('login.html', form = form)

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))
     
@app.route("/")
def main():


    initsync_processes = session.query(ProcessControl.id, ProcessControl.profile_name, ProcessControl.profile_version, ProcessControl.process_code, ProcessControl.status, ProcessControl.total_count, ProcessControl.duration, ProcessControl.process_starttime, ProcessControl.process_endtime, func.count(ProcessControlDetail.id).label('total')).filter(ProcessControlDetail.comment.like('%Finished%')).filter(ProcessControl.process_code == 'InitSync').join(ProcessControlDetail).group_by(ProcessControl.id).order_by(ProcessControl.id.desc())

    cdc_processes = session.query(ProcessControl.id, ProcessControl.profile_name, ProcessControl.profile_version, ProcessControl.process_code, ProcessControl.status, ProcessControl.total_count, ProcessControl.duration, ProcessControl.process_starttime, ProcessControl.process_endtime, func.sum(ProcessControlDetail.source_row_count).label('total')).filter(ProcessControl.process_code != 'InitSync').join(ProcessControlDetail).group_by(ProcessControl.id).order_by(ProcessControl.id.desc())
    
    # pass the list of process control table entries to html
    return render_template('index.html', cdc_processes = cdc_processes,initsync_processes = initsync_processes )
       
       
@app.route("/processlist", methods=['GET','POST'])
def processlist():

    q = session.query(ProcessControl)
    processControl = q.order_by(ProcessControl.id.desc())
    
    if request.method == 'POST' :

        # apply filters
        filter_profile = request.form['profile']
        filter_sourcesystem = request.form['sourcesystem']
        filter_status = request.form['status']
        
        if filter_profile:
           q = q.filter(ProcessControl.profile_name==filter_profile)       
     
        if filter_status:
           if filter_status != 'ALL':
              q = q.filter(ProcessControl.status==filter_status)    

        if filter_sourcesystem:
           q = q.filter(ProcessControl.source_system_code==filter_sourcesystem)    
           
        processControl = q.order_by(ProcessControl.id.desc())

    # pass the list of process control table entries to html
    return render_template('process_control_list.html', processControl = processControl)
    
    
@app.route("/processdetails/<process_id>/", methods=['GET','POST'])
def processdetails(process_id):

    processControlDetail = session.query(ProcessControlDetail).filter(ProcessControlDetail.run_id == process_id)
    p = session.query(ProcessControlDetail).filter(ProcessControlDetail.run_id == process_id).first()
    if p: 
        process = p.process_code
    else:
        process = 'No details found!'

    if request.method == 'POST':
        # get the order by field name from select list
        order_by = request.form['order_by']
        # default order is by object name
        if order_by == 'status':
           processControlDetail = processControlDetail.order_by(ProcessControlDetail.status)
        if order_by == 'process_starttime':
           processControlDetail = processControlDetail.order_by(ProcessControlDetail.process_starttime)
    else:
        processControlDetail = processControlDetail.order_by(ProcessControlDetail.object_name, ProcessControlDetail.process_code.desc())
    # pass the list of process control  detail table entries to html
    return render_template('process_control_details.html', processControlDetail = processControlDetail, runid = process_id, process = process)

@app.route("/profilelist", methods=['GET','POST'])
def profilelist():


    profiles = session.query(Profile).order_by(Profile.profile_name, Profile.version)

    names = session.query(Profile).distinct(Profile.profile_name).order_by(Profile.profile_name)
           
    if request.method == 'POST' :
 
        # apply filters      
        filter_profile = request.form['profile']
        filter_version = request.form['version']
        filter_sourcesystem = request.form['sourcesystem']
    
        if filter_profile:
           if filter_profile != 'ALL':
             profiles = profiles.filter(Profile.profile_name==filter_profile)       
     
        if filter_version:
           profiles = profiles.filter(Profile.version==filter_version)    

        if filter_sourcesystem:
           profiles = profiles.filter(Profile.source_system_code==filter_sourcesystem)          
    
    # pass the list of source system profile table entries to html
    return render_template('profile_list.html', profiles = profiles, names = names)


@app.route("/profileadd", methods=['GET','POST'])
@login_required
def profileadd():
   
    # create an empty object
    profile = Profile()

    # initialize the form
    form = ProfileHeaderForm(request.form, obj=profile)

    form.source_connection.query = session.query(Connections.connection_name).order_by(Connections.connection_name)
    if request.method == 'POST' and form.validate_on_submit():
       
        # The request is POST
        # get the values from form and save

        form.populate_obj(profile)

        # Update date time fields from System date time values
        # now = datetime.now() 
        # profile.created_date = now

        # TODO: this method will be available to admin users only. 
        # so the following is for test implementation only

        # profile.created_by = 'admin'

        try: 
            session.add(profile)
            session.commit()    
        except Exception as e: #catch all exceptions
           print( "Session Commit Errors: %s" % str(e) )
           
        # go to select schema page
        return redirect(url_for('schemalist',profile_id=profile.id))
       
    return render_template('profile_add.html', form = form)
    
@app.route("/profileupdate/<profile_id>/", methods=['GET','POST'])
def profileupdate(profile_id):
      
    profile = session.query(Profile).filter(Profile.id == profile_id).first()
    
    form = ProfileHeaderForm(request.form, obj=profile)
    
    form.source_connection.query = session.query(Connections.connection_name).order_by(Connections.connection_name)
   
    if request.method == 'POST' and form.validate_on_submit():

        # The request is POST
        # get the values from form and save
        
        form.populate_obj(profile)
        
        session.flush
        session.commit()
        
        return redirect(url_for('profilelist'))
    
    # The request is GET    
    return render_template('profile_update.html', form = form)
    
@app.route("/profileobjects/<profile_name>/<version>")
def profileobjects(profile_name,version):

    profile_header = session.query(Profile).filter(Profile.profile_name == profile_name, Profile.version == version ).first()
    
    profile_details = session.query(SourceSystemProfile).filter(SourceSystemProfile.profile_name == profile_name, SourceSystemProfile.version == version ).order_by(SourceSystemProfile.object_seq)
 
    # pass the list of process control  detail table entries to html
    return render_template('profile_objects_list.html', profile_details = profile_details, profile_header = profile_header)

@app.route("/profileitemupdate/<profile_id>/", methods=['GET','POST'])
def profileitemupdate(profile_id):
   
    profile = session.query(SourceSystemProfile).filter(SourceSystemProfile.id == profile_id).first()

    form = ProfileItemForm(request.form, obj=profile)

    if request.method == 'POST' and form.validate_on_submit():

        # The request is POST
        # get the values from form and save
        form.populate_obj(profile)
        
        session.flush
        session.commit()
        
        return redirect(url_for('profileobjects',profile_name=profile.profile_name,version=profile.version))
    
    # The request is GET    
    return render_template('profile_item_update.html', form = form, profile = profile)
     
@app.route("/profileitemdelete/<profile_id>/", methods=['GET','POST'])
@login_required
def profileitemdelete(profile_id):
   
    profile = session.query(SourceSystemProfile).filter(SourceSystemProfile.id == profile_id).first()

    if request.method == 'POST':

        # The request is POST and DELETE is confirmed
        # Delete the curent record 
        session.delete(profile)
        session.commit()
        
        return redirect(url_for('profileobjects',profile_name=profile.profile_name,version=profile.version))
    
    # The request is GET    
    return render_template('profile_item_delete.html', profile = profile)
     
     
@app.route("/profileitemadd/<profile_name>/<version>")
@login_required
def profileitemadd(profile_name,version):
   
    profile = session.query(SourceSystemProfile).filter(SourceSystemProfile.profile_name == profile_name, SourceSystemProfile.version == version ).first()
    # get the profile name and version for next query 
    
    # Find the next object_seq number
    profile_details = session.query(SourceSystemProfile).filter(SourceSystemProfile.profile_name == profile_name, SourceSystemProfile.version == version ).order_by(SourceSystemProfile.object_seq.desc()).first()
    next_object_seq = profile_details.object_seq + 1  
        
    new_profile_item = SourceSystemProfile()
    
    # inherit header fields from parent record
    new_profile_item.profile_name = profile.profile_name
    new_profile_item.version = version
    new_profile_item.source_system_code = profile.source_system_code
    new_profile_item.source_region = profile.source_region
    new_profile_item.target_region = profile.target_region

    # Default values
    new_profile_item.active_ind = 'Y'
    # allocate the object sequence
    new_profile_item.object_seq = next_object_seq  
    
    form = ProfileItemForm(request.form, obj=new_profile_item)
    
    if request.method == 'POST' and form.validate_on_submit():
       
        # The request is POST
        # get the values from form and save
        form.populate_obj(new_profile_item)
        
        # Update date time fields from System date time values
        now = datetime.now() 
        new_profile_item.last_updated = now
        new_profile_item.last_applied = now 
        new_profile_item.last_history_update = now
        
        session.add(new_profile_item)
        session.commit()    
       
        return redirect(url_for('profileobjects',profile_name=profile.profile_name,version=profile.version))    
    
    # The request is GET  
   
    return render_template('profile_item_add.html', form = form, profile=profile)    
    
@app.route("/connections",  methods=['GET','POST'])
def connections():

    connections = session.query(Connections).order_by(Connections.connection_name)
    
    if request.method == 'POST' :
 
        # apply filters
        
        filter_category = request.form['category']
        filter_database = request.form['database']
        
        if filter_category:
           if filter_category != 'ALL':
              connections = connections.filter(Connections.connection_category==filter_category)       
     
        if filter_database:
           if filter_database != 'ALL':
               connections = connections.filter(Connections.database_type==filter_database)    

    
    # pass the list of connection table entries to html
    return render_template('connections_list.html', connections = connections)

@app.route("/addconnection", methods=['GET','POST'])
@login_required
def addconnection():
   
    # create an empty object
    connection = Connections()
    
    # initialize the form
    form = ConnectionForm(request.form, obj=connection)
    
    if request.method == 'POST' and form.validate_on_submit():
       
        # The request is POST
        # get the values from form and save
        form.populate_obj(connection)
        
        # Update date time fields from System date time values
        now = datetime.now() 
        connection.created_date = now

        # TODO: this method will be availbale to admin users only. 
        # so the following is for test implementation only

        connection.created_by = 'admin'
       
        session.add(connection)
        session.commit()    
        
        # go back to Connections List view page
        return redirect(url_for('connections'))
       
    return render_template('connections_add.html', form = form)



@app.route("/updateconnection/<connection_id>/", methods=['GET','POST'])
@login_required
def updateconnection(connection_id):
   
    connection = session.query(Connections).filter(Connections.id == connection_id).first()

    form = ConnectionForm(request.form, obj=connection)

    if request.method == 'POST' and form.validate_on_submit():

        # The request is POST
        # get the values from form and save
        form.populate_obj(connection)
        
        session.flush
        session.commit()
        
        return redirect(url_for('connections'))
    
    # The request is GET    
    return render_template('connections_update.html', form = form, connection = connection)
 
@app.route("/browseconnection/<connection_name>/", methods=['GET','POST'])
@login_required
def browseconnection(connection_name):
    
    connection = session.query(Connections).filter(Connections.connection_name == connection_name).first()
    
    #TODO Handle exception if connection is not found, FK integrity issue   
    # connection for schemas    
    conn_details = db_connection.ConnectionDetails(connection.username,connection.password, connection.hostname, connection.portnumber, connection.database_name)
    
    conn_schema = db_factory.build(connection.database_type.lower())    
    conn_schema.connect(conn_details)    
    
    #schema_query = "SELECT distinct owner FROM ALL_TABLES ORDER BY 1 "
    schema_query = _db_schema_query(connection.database_type.lower())    
    schemas = conn_schema.execute_query(schema_query, 1000 )    
    
    # connection for objects
    conn_objects = db_factory.build(connection.database_type.lower())    
    conn_objects.connect(conn_details)
            
    #object_query = "SELECT table_name from ALL_TABLES WHERE owner ='" + connection.username + "' ORDER BY 1"
    object_query = _db_objects_query(connection.database_type.lower(),connection.username)
    objects = conn_objects.execute_query(object_query, 1000 )
    
    if request.method == 'POST' :
 
        # apply filters      
        filter_schema_name = request.form['schema_name']
  
        if filter_schema_name:
             #query = "SELECT table_name from ALL_TABLES WHERE owner ='" + filter_schema_name + "'"             
             object_query = _db_objects_query(connection.database_type.lower(),filter_schema_name)
             objects = conn_objects.execute_query(object_query, 1000 )
          
    # pass the list of source system profile table entries to html
    return render_template('connections_browse.html', schemas = schemas, objects = objects)
        
@app.route("/users",  methods=['GET','POST'])
@login_required
def users():
    if current_user.get_role() != 'Admin':
        return render_template('users_message.html', message = 'Admin role is required')   
        
    users = session.query(User).order_by(User.lastname)
    
    if request.method == 'POST' :
 
        # apply filters
        
        filter_role = request.form['role']
        
        if filter_role:
           if filter_role != 'ALL':
              users = users.filter(User.role==filter_role)       
    
    # pass the list of connection table entries to html
    return render_template('users_list.html', users = users)

@app.route("/adduser", methods=['GET','POST'])
@login_required
def adduser():
   
    # create an empty object
    user = User()
    
    # initialize the form
    form = UserForm(request.form, obj=user)
    
    if request.method == 'POST' and form.validate_on_submit():
       
        # The request is POST
        # get the values from form and save
        form.populate_obj(user)
        
        # Update date time fields from System date time values
        now = datetime.now() 
        user.registered_on = now
       
        session.add(user)
        session.commit()    
        
        # go back to Users List view page
        return redirect(url_for('users'))
       
    return render_template('users_add.html', form = form)



@app.route("/updateuser/<user_id>/", methods=['GET','POST'])
@login_required
def updateuser(user_id):
   
    user = session.query(User).filter(User.id == user_id).first()

    form = UserForm(request.form, obj=user)

    if request.method == 'POST' and form.validate_on_submit():

        # The request is POST
        # get the values from form and save
        form.populate_obj(user)
        
        session.flush
        session.commit()
        
        return redirect(url_for('users'))
    
    # The request is GET    
    return render_template('users_update.html', form = form, user = user)
 
@app.route("/parameters",  methods=['GET','POST'])
def parameters():

    parameters = session.query(ProcessParameters).order_by(ProcessParameters.parameter_name)
    
    if request.method == 'POST' :
 
        # apply filters
        
        filter_type = request.form['parameter_type']
        
        if filter_type:
           if filter_type != 'ALL':
              parameters = parameters.filter(ProcessParameters.parameter_type==filter_type)       
         
    # pass the list of parameters from table entries to html
    return render_template('parameters_list.html', parameters = parameters)

@app.route("/addparameter", methods=['GET','POST'])
def addparameter():
   
    # create an empty object
    parameter = ProcessParameters()
    
    # initialize the form
    form = ParameterForm(request.form, obj=parameter)
    
    if request.method == 'POST' and form.validate_on_submit():
       
        # The request is POST
        # get the values from form and save
        form.populate_obj(parameter)
        
        # TODO: Update date time fields from System date time values
        # now = datetime.now() 
        # parameter.created_date = now

        # TODO: this method will be available to admin users only. 
        # so the following is for test implementation only
        # parameter.created_by = 'admin'
        session.add(parameter)
        session.commit()  
       
        # go back to Connections List view page
        return redirect(url_for('parameters'))
       
    return render_template('parameters_add.html', form = form)



@app.route("/updateparameter/<parameter_id>/", methods=['GET','POST'])
def updateparameter(parameter_id):
   
    parameter = session.query(ProcessParameters).filter(ProcessParameters.id == parameter_id).first()

    form = ParameterForm(request.form, obj=parameter)

    if request.method == 'POST' and form.validate_on_submit():

        # The request is POST
        # get the values from form and save
        form.populate_obj(parameter)
        
        session.flush
        session.commit()
        
        return redirect(url_for('parameters'))
    
    # The request is GET    
    return render_template('parameters_update.html', form = form, parameter = parameter)
 
    
@app.route('/viewfile/<file_name>')
def viewfile(file_name):
    # Slashed are replaced by pipes in file_name So Reconstruct it
    file_name = file_name.replace("|","/")
  
    # file_name contains log file name with full path
    
    if os.path.isfile(file_name):
       f = open(file_name, 'r')
       file_contents = f.read()
       f.close()
    else:
       file_contents = "File Not found!!!"
    
    # pass the list of process control  detail table entries to html
    return render_template('view_file.html', file_contents = file_contents, file_name = file_name)
  
# TODO Follwong code is for demo purposes at the moment   

@app.route("/schemalist/<profile_id>/", methods=['GET','POST'])
def schemalist(profile_id):
  
    profile = session.query(Profile).filter(Profile.id == profile_id).first()

    connection = session.query(Connections).filter(Connections.connection_name == profile.source_connection).first()
       
    #TODO Handle exception if connection is not found, FK integrity issue    
    conn_details = db_connection.ConnectionDetails(connection.username,connection.password, connection.hostname, connection.portnumber, connection.database_name)
    
    conn_schema = db_factory.build(connection.database_type.lower())    
    conn_schema.connect(conn_details)    
    
    #schema_query = "SELECT distinct owner FROM ALL_TABLES ORDER BY 1 "
    schema_query = _db_schema_query(connection.database_type.lower())    
    schemas = conn_schema.execute_query(schema_query, 1000 )    
              
    if request.method == 'POST' :
 
        schema_list = ""
        # construct a list of selected schemas
        for schema in schemas:          
            if request.form.get(schema[0]) == 'Y':
                schema_list = schema_list + "'" + schema[0] + "',"
        schema_list = schema_list + "''"
        # print schema_list
            
        if schema_list:
                    
            # go to Obkjects List view page
            return redirect(url_for('objectlist',connection_name=connection.connection_name,schema_list=schema_list,profile_id=profile_id))

    
    # pass the list of source system profile table entries to html
    return render_template('profile_schema_list.html', schemas = schemas)
    
    
@app.route("/objectlist/<connection_name>/<schema_list>/<profile_id>/", methods=['GET','POST'])
def objectlist(connection_name,schema_list,profile_id):
    
    profile = session.query(Profile).filter(Profile.id == profile_id).first()
        
    connection = session.query(Connections).filter(Connections.connection_name == connection_name).first()
       
    #TODO Handle exception if connection is not found, FK integrity issue    
    conn_details = db_connection.ConnectionDetails(connection.username,connection.password, connection.hostname, connection.portnumber, connection.database_name)
     
    conn_objects = db_factory.build(connection.database_type.lower())    
    conn_objects.connect(conn_details)

    # query = "SELECT owner, table_name from ALL_TABLES WHERE owner IN (" + schema_list  + ") order by 1, 2"    
    object_query = _db_objects_in_schemas_query(connection.database_type.lower(),schema_list)
    objects = conn_objects.execute_query(object_query, 1000 )
    
           
    if request.method == 'POST' :
        
        object_seq = 1
        # Process selected objects
        for object in objects:  
            # checkbox name is schema|tablename        
            input_name=object[0] + "|" + object[1]   
            
            # check if the object is selected
            if request.form.get(input_name) == 'Y':
            
                # table is chosen to be replicated
                # Create an entry in source system profile 
                new_profile_item = SourceSystemProfile()
                
                new_profile_item.source_region = object[0]
                new_profile_item.object_name = object[1]
                
                # allocate the object sequence
                new_profile_item.object_seq = object_seq  
             
                # inherit header fields from parent profile record
                new_profile_item.profile_name = profile.profile_name
                new_profile_item.version = profile.version
                new_profile_item.source_system_code = profile.source_system_code

                # Default values
                new_profile_item.active_ind = 'Y'
                new_profile_item.min_lsn = "0"
                new_profile_item.max_lsn = "0"  
                new_profile_item.last_status = "New"
                new_profile_item.last_run_id = 0    
                new_profile_item.last_process_code = "" 
                
                # Update date time fields from System date time values
                now = datetime.now() 
                new_profile_item.last_updated = now
                new_profile_item.last_applied = now 
                new_profile_item.last_history_update = now
        
                session.add(new_profile_item)
                session.commit()   

                object_seq = object_seq + 1
                   
        return redirect(url_for('profilelist'))         
    # pass the list of source system profile table entries to html
    return render_template('profile_object_list.html', objects = objects)

@app.route("/profileitemschemalist/<profile_id>/", methods=['GET','POST'])
def profileitemschemalist(profile_id):
  
    profile = session.query(Profile).filter(Profile.id == profile_id).first()

    connection = session.query(Connections).filter(Connections.connection_name == profile.source_connection).first()
       
    #TODO Handle exception if connection is not found, FK integrity issue    
    conn_details = db_connection.ConnectionDetails(connection.username,connection.password, connection.hostname, connection.portnumber, connection.database_name)
    
    conn_schema = db_factory.build(connection.database_type.lower())    
    conn_schema.connect(conn_details)    
    
    #schema_query = "SELECT distinct owner FROM ALL_TABLES ORDER BY 1 "
    schema_query = _db_schema_query(connection.database_type.lower())    
    schemas = conn_schema.execute_query(schema_query, 1000 )    
              
    if request.method == 'POST' :
 
        schema_list = ""
        # construct a list of selected schemas
        for schema in schemas:          
            if request.form.get(schema[0]) == 'Y':
                schema_list = schema_list + "'" + schema[0] + "',"
        schema_list = schema_list + "''"
        # print schema_list
            
        if schema_list:
                    
            # go to Obkjects List view page
            return redirect(url_for('profileitemobjectlist',connection_name=connection.connection_name,schema_list=schema_list,profile_id=profile_id))

    
    # pass the list of source system profile table entries to html
    return render_template('profile_item_schema_list.html', schemas = schemas)
    
    
@app.route("/profileitemobjectlist/<connection_name>/<schema_list>/<profile_id>/", methods=['GET','POST'])
def profileitemobjectlist(connection_name,schema_list,profile_id):
    
    profile = session.query(Profile).filter(Profile.id == profile_id).first()
        
    connection = session.query(Connections).filter(Connections.connection_name == connection_name).first()
       
    #TODO Handle exception if connection is not found, FK integrity issue    
    conn_details = db_connection.ConnectionDetails(connection.username,connection.password, connection.hostname, connection.portnumber, connection.database_name)
     
    conn_objects = db_factory.build(connection.database_type.lower())    
    conn_objects.connect(conn_details)

    # query = "SELECT owner, table_name from ALL_TABLES WHERE owner IN (" + schema_list  + ") order by 1, 2"    
    object_query = _db_objects_in_schemas_query(connection.database_type.lower(),schema_list)
    objects = conn_objects.execute_query(object_query, 1000 )
           
    if request.method == 'POST' :
        
        # Find the next object_seq number
        profile_details = session.query(SourceSystemProfile).filter(SourceSystemProfile.profile_name == profile.profile_name, SourceSystemProfile.version == profile.version ).order_by(SourceSystemProfile.object_seq.desc()).first()
        
        next_object_seq = profile_details.object_seq + 1 
        
        # Process selected objects
        for object in objects:  
            # checkbox name is schema|tablename        
            input_name=object[0] + "|" + object[1]   
            
            # check if the object is selected
            if request.form.get(input_name) == 'Y':
            
                # table is chosen to be replicated
                # Create an entry in source system profile 
                new_profile_item = SourceSystemProfile()
                
                new_profile_item.source_region = object[0]
                new_profile_item.object_name = object[1]
                
                # allocate the object sequence
                new_profile_item.object_seq = next_object_seq  
             
                # inherit header fields from parent profile record
                new_profile_item.profile_name = profile.profile_name
                new_profile_item.version = profile.version
                new_profile_item.source_system_code = profile.source_system_code

                # Default values
                new_profile_item.active_ind = 'Y'
                new_profile_item.min_lsn = "0"
                new_profile_item.max_lsn = "0"  
                new_profile_item.last_status = "New"
                new_profile_item.last_run_id = 0    
                new_profile_item.last_process_code = "" 
                
                # Update date time fields from System date time values
                now = datetime.now() 
                new_profile_item.last_updated = now
                new_profile_item.last_applied = now 
                new_profile_item.last_history_update = now
        
                session.add(new_profile_item)
                session.commit()   

                next_object_seq = next_object_seq + 1
                   
        return redirect(url_for('profilelist'))         
    # pass the list of source system profile table entries to html
    return render_template('profile_item_object_list.html', objects = objects)

@app.route("/charts")
def charts():
    legend = 'Monthly Data'
    labels = ["January","February","March","April","May","June","July","August"]
    values = [10,9,8,7,6,4,7,8]
    colors = [ "#F7464A", "#46BFBD", "#FDB45C", "#FEDCBA","#ABCDEF", "#DDDDDD", "#ABCABC"  ]
    return render_template('charts.html', values=values, labels=labels, colors=colors, legend=legend)

@app.route("/files")
def listfiles():

    files = os.listdir("/tmp/files")

    return render_template('list_files.html', files = files)

@app.route("/run/<profile_id>")
@login_required
def runcommand(profile_id):

    profile = session.query(Profile).filter(Profile.id == profile_id).first()    
    # command is the command script name with full path
    command = profile.server_path
    
    # Default output text if command runs successfully
    out = "Job " + command + " Submitted successfully, Check the progress from Process List"
    
    # Check if command exists on server
    if os.path.isfile(command):
        try:
            command = [command, "--newinitsync"]
            # run the command with newinitsync argument - Non blocking call
            p = subprocess.Popen(command, stdout = None,
                            stderr=None,
                            stdin=None)
                            
        except Exception as e:
            out = e.message
        
    else:
        # Script is not found
        out = "Script " + command + "Not found!!!"

    return render_template('command_output.html', out = out)

@app.route("/scheduleinitsync/<profile_id>", methods=['GET','POST'])
@login_required
def scheduleinitsync(profile_id):
    
    profile = session.query(Profile).filter(Profile.id == profile_id).first()  
    # command is the command script name with full path
    command = profile.server_path
    
    if request.method == 'POST' :

        # replication tyoe is either initsync or cdc
        replication_type = "initsync"
    
        if (request.form['schedule_type'] == 'oneoff'):
        
            if (request.form['run_date'] and request.form['run_time'] ):
               run_datetime = request.form['run_date'] + ' ' + request.form['run_time']
            else:
                # default run immediately i.e. in a minute            
                run_datetime = datetime.now() + timedelta(seconds=60)
            
            #add one off job  
            job_name = profile.profile_name.strip() +"-" + str(profile.version) + "-oneoff-sync"
            scheduler.add_job(oneoff, 'date', args=[command,replication_type], name=job_name, run_date=run_datetime)
            
        else:
        
            # scheduled recurring job
            job_name = profile.profile_name.strip() + "-" + str(profile.version) +    "-recurring-sync"     
            run_days = request.form['run_days']
            run_hours = request.form['run_hours']
            run_minutes = request.form['run_minutes']
            
            # Defaults are *, *, 00 if they are left empty
            if run_days == "":
               run_days = '*'
            if run_hours == "":
               run_hours = '*'  
            if run_minutes == "":
               run_minutes = '00'
                
            #add cron job
            scheduler.add_job(recurring, 'cron', args=[command,replication_type], name=job_name, day_of_week=run_days, hour=run_hours, minute=run_minutes)
          
        return redirect(url_for('profilelist'))  

    return render_template('profile_schedule_create_initsync.html',profile=profile)

@app.route("/schedulecdc/<profile_id>", methods=['GET','POST'])
@login_required
def schedulecdc(profile_id):
    
    profile = session.query(Profile).filter(Profile.id == profile_id).first()  
    # command is the command script name with full path
    command = profile.server_path
    
    if request.method == 'POST' :

        # replication tyoe is either initsync or cdc
        replication_type = "cdc"

        job_name = profile.profile_name.strip() + "-" + str(profile.version) +    "-recurring-cdc"     
        run_days = request.form['run_days']
        run_hours = request.form['run_hours']
        run_minutes = request.form['run_minutes']
        
        # Defaults are *, *, 00 if they are left empty
        if run_days == "":
            run_days = '*'
        if run_hours == "":
           run_hours = '*'  
        if run_minutes == "":
           run_minutes = '00'
            
        #add cron job
        scheduler.add_job(recurring, 'cron', args=[command,replication_type], name=job_name, day_of_week=run_days, hour=run_hours, minute=run_minutes)
    
        
        return redirect(url_for('profilelist'))  

    return render_template('profile_schedule_create_cdc.html',profile=profile)
    
@app.route("/schedulelist", methods=['GET','POST'])
def schedulelist():
    
    jobs = scheduler.get_jobs()
    
    if request.method == 'POST' :
        # TODO Filtering list view
        job_filter = request.form['job_name']
        if job_filter != "":
            filtered_jobs = []
            for job in jobs:
                if job.name.lower().find(job_filter.lower()) >= 0:
                    filtered_jobs.append(job)
            return render_template('profile_schedule_list_all.html', jobs=filtered_jobs)      

    return render_template('profile_schedule_list_all.html', jobs=jobs)  

@app.route("/profileschedulelist/<profile_id>")
def profileschedulelist(profile_id):
    
    profile = session.query(Profile).filter(Profile.id == profile_id).first()  
    # command is the command script name with full path
    command = profile.server_path
    
    jobs = scheduler.get_jobs()
    
    return render_template('profile_schedule_list.html', jobs=jobs, profile=profile)  
    
@app.route("/profilescheduledelete/<job_id>/<profile_id>", methods=['GET','POST'])
@login_required
def profilescheduledelete(job_id,profile_id):
   
    # find the job details
    jobs = scheduler.get_jobs()
    for job in jobs:
        if (job.id==job_id):
            break
            
    if request.method == 'POST':

        # The request is POST and DELETE is confirmed
        # Delete the curent record 

        scheduler.remove_job(job_id)
        
        # log the details for job remove
        # print('job deleted: %s trigger: %s nextrun: %s' % (job.name, job.trigger, job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")))

        return redirect(url_for('profileschedulelist',profile_id=profile_id))      

    
    # The request is GET    
    return render_template('profile_schedule_list_delete.html', job = job,profile_id=profile_id)

@app.route("/scheduledelete/<job_id>", methods=['GET','POST'])
@login_required
def scheduledelete(job_id):
   
    # find the job details
    jobs = scheduler.get_jobs()
    for job in jobs:
        if (job.id==job_id):
            break
            
    if request.method == 'POST':

        # The request is POST and DELETE is confirmed
        # Delete the curent record 

        scheduler.remove_job(job_id)
        
        # log the details for job remove
        # print('job deleted: %s trigger: %s nextrun: %s' % (job.name, job.trigger, job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")))
        
        return redirect(url_for('schedulelist'))
    
    # The request is GET    
    return render_template('profile_schedule_list_all_delete.html', job = job)
    
def _db_schema_query(dbtype_name):
    
    query = None
    if dbtype_name == const.ORACLE:
        query = "SELECT distinct owner as schema_name FROM ALL_TABLES ORDER BY 1 "
    elif dbtype_name == const.MSSQL:
        query = "SELECT distinct table_schema as schema_name FROM INFORMATION_SCHEMA.COLUMNS;"
    return query

def _db_objects_query(dbtype_name, schema_name):
    
    query = None
    if dbtype_name == const.ORACLE:
        query = "SELECT table_name from ALL_TABLES WHERE owner ='" + schema_name + "' ORDER BY 1"
    elif dbtype_name == const.MSSQL:
        query = "SELECT distinct table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '" + schema_name + "' ORDER BY 1"
    return query    

def _db_objects_in_schemas_query(dbtype_name, schema_list):
    
    query = None
    if dbtype_name == const.ORACLE:
        query ="SELECT owner as schema_name, table_name from ALL_TABLES WHERE owner IN (" + schema_list  + ") order by 1, 2"
    elif dbtype_name == const.MSSQL:
        query = "SELECT distinct table_schema as schema_name, table_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema IN (" + schema_list + ") ORDER BY 1,2"
    return query    


def oneoff(command,replication_type):
    # Slashed are replaced by pipes in command path So Reconstruct it
    # command is the command script name to run with full path
    command = command.replace("|","/")
    
     # Check if command exists on server
    if os.path.isfile(command):
        try:
            # this function is applicable if replication type is initsync
            if replication_type == "initsync":
                command = [command, "--newinitsync"]
                # run the command - Non blocking call
                p = subprocess.Popen(command, stdout = None,stderr=None,stdin=None)
                print('Scheduled one off %s.' % command)         
            else:
                # replication type CDC, ignore to schedule the job
                print('The replication type is CDC. This feature is not available')         
                
        except Exception as e:
                out = e.message           
    else:
        # Script is not found
        out = "Script " + command + "Not found!!!"
        print('%s.' % out)

def recurring(command,replication_type):
    # Slashed are replaced by pipes in command path So Reconstruct it
    # command is the command script name to run with full path
    command = command.replace("|","/")
    
     # Check if command exists on server
    if os.path.isfile(command):
        try:
            if replication_type == "initsync":
                command = [command, "--newinitsync"]
                # run the command - Non blocking call
                p = subprocess.Popen(command, stdout = None,stderr=None,stdin=None)
            else:
                # CDC run extractor and applier in parralle
                # TO DO check if applier id already running 
                # or add safety valve to script to avoid re-run
                command = [command, "--extractor"]
                # run the command - Non blocking call
                p = subprocess.Popen(command, stdout = None,stderr=None,stdin=None)
                command = [command, "--applier"]
                # run the command - Non blocking call
                p = subprocess.Popen(command, stdout = None,stderr=None,stdin=None)
                                         
        except Exception as e:
                out = e.message           
    else:
        # Script is not found
        out = "Script " + command + "Not found!!!"
    print('Scheduled recurring %s.' % command)

    
if __name__ == "__main__":
   
   # start Scheduler
   scheduler.start()


   app.config["SECRET_KEY"] = "ITSASECRET"
   # start the web server on all IPs(0.0.0.0) at port 5000 as default
   app.run(host=argv.httphost, port=argv.httpport)
