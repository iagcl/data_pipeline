
from datetime import datetime, timedelta
import time
import sys
import os



from apscheduler.schedulers.background import BackgroundScheduler





def tick():

    print('Tick! The time is: %s' % datetime.now())


def alarm(time):

    print('Alarm! This alarm was scheduled at %s.' % time)

def job_function():

    print('Hello! The time is: %s' % datetime.now())





if __name__ == '__main__':

    scheduler = BackgroundScheduler()

    url = sys.argv[1] if len(sys.argv) > 1 else 'sqlite:///example.sqlite'

    scheduler.add_jobstore('sqlalchemy', url=url)
    
    #interval example
    scheduler.add_job(tick, 'interval', seconds=5)
    
    #at a specific time example
    alarm_time = datetime.now() + timedelta(seconds=10)
    scheduler.add_job(alarm, 'date', run_date=alarm_time, args=[datetime.now()])

    #add cron job
    scheduler.add_job(job_function, 'cron', minute='37,39,40')
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    scheduler.print_jobs()
    
    jobs = scheduler.get_jobs()
    
    for job in jobs:
       print('job: %s trigger: %s nextrun: %s' % (job.name, job.trigger, job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")))
       

    try:

        # This is here to simulate application activity (which keeps the main thread alive).

        while True:

            time.sleep(2)

    except (KeyboardInterrupt, SystemExit):

        # Not strictly necessary if daemonic mode is enabled but should be done if possible

        scheduler.shutdown()