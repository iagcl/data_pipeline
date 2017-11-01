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


#pip install apscheduler

from datetime import datetime, timedelta
import sys
import os

from apscheduler.schedulers.blocking import BlockingScheduler

def alarm(time):

    print('Alarm! This alarm was scheduled at %s.' % time)

if __name__ == '__main__':

    scheduler = BlockingScheduler()

    url = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:///example.sqlite'

    scheduler.add_jobstore('sqlalchemy', url=url)

    alarm_time = datetime.now() + timedelta(seconds=10)

    scheduler.add_job(alarm, 'date', run_date=alarm_time, args=[datetime.now()])

    print('To clear the alarms, delete the example.sqlite file.')

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:

        scheduler.start()

    except (KeyboardInterrupt, SystemExit):

        pass
        

        
        