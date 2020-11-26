#!/usr/bin/python3

import boto3, glob, datetime, logging, os
from datetime import datetime, timedelta

#vars
backups_bkt = 'mparticle-backups'
local_dir = '/tmp/mpart/'
#initialize s3
s3 = boto3.resource('s3')
os.chdir(local_dir)
to_upload = glob.glob('*')
bkt = s3.Bucket(backups_bkt)

#datetime
log_dt = datetime.today().strftime('%Y%m%d')
bp_dt = datetime.today().strftime('%Y%m%d-%H:%M:%S')
#7 days back date
#week_old = datetime.today() - timedelta(7)
#week_old = week_old.strftime('%Y%m%d')

#logging setup
logging.basicConfig(filename=f'/home/ubuntu/mp_backup_{log_dt}.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

#email setup


#create bucket if doesnt exist
def upload_s3():
    global failed_files
    failed_files = []
    failed_files = ['yo123456']
    try:
        for i in to_upload:
            bkt.put_object(Key=f'{i}_{bp_dt}')
            logging.info(f"Uploaded: {i} to {backups_bkt}")
    except Exception as e:
        logging.error(f"Backup/Confirmation failed for {i} due to: {e}")

def verify_s3():
    try:
        for i in bkt.objects.all():
            for b in to_upload:
                if b in i.key:
                    logging.info(f"Verified: {i.key} exists in {backups_bkt}")
    except Exception as e:
        logging.error(f"Backup/Confirmation failed for {i} due to: {e}")
        failed_files.append(i)

#delete backups older than 7 days
def clean_s3():
    for i in bkt.objects.all():
        for d in range(8,30):
            week_old = datetime.today() - timedelta(d)
            week_old = week_old.strftime('%Y%m%d')
            if week_old in i.key:
                logging.info(f"Deleted: File older than 7 days: {i.key}")
                s3.Object(backups_bkt, i.key).delete()

def email_notify():
    logging.info(f"{len(failed_files)} files failed to upload.")
    if (len(failed_files)) > 0:
        for i in failed_files:
            logging.error(f"{i} failed to upload")

upload_s3()
verify_s3()
clean_s3()
email_notify()
