

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
        

        
        