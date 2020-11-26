#!/usr/bin/python3

import boto3, glob, datetime, logging, os, smtplib, configparser
from datetime import datetime, timedelta

#config setup
conf = configparser.ConfigParser()
conf.read('mp_backup.ini')
#vars
backups_bkt = conf['DEFAULT']['backups_bucket']
local_dir = conf['DEFAULT']['local_dir']
log_file = conf['DEFAULT']['log_file']
email_from = conf['EMAIL']['email_from']
email_to = conf['EMAIL']['email_to']

#initialize s3
s3 = boto3.resource('s3')
os.chdir(local_dir)
to_upload = glob.glob('*')
bkt = s3.Bucket(backups_bkt)

#datetime
log_dt = datetime.today().strftime('%Y%m%d')
bp_dt = datetime.today().strftime('%Y%m%d-%H:%M:%S')

#logging setup
logging.basicConfig(filename=f'{log_file}_{log_dt}.log', filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

#upload to s3
def upload_s3():
    global failed_files
    failed_files = []
    try:
        for i in to_upload:
            bkt.put_object(Key=f'{bp_dt}_{i}')
            logging.info(f"Uploaded: {i} to {backups_bkt}")
    except Exception as e:
        logging.error(f"Backup/Confirmation failed for {i} due to: {e}")

#verify files exist in s3
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
        #I could have found a way to sort s3 files by modified time but thought this would just be a fine way for now
        for d in range(8,30):
            week_old = datetime.today() - timedelta(d)
            week_old = week_old.strftime('%Y%m%d')
            if week_old in i.key:
                logging.info(f"Deleted: File older than 7 days: {i.key}")
                s3.Object(backups_bkt, i.key).delete()

#send email status and log final result
def email_notify():

    logging.info(f"{len(failed_files)} files failed to upload.")
    if (len(failed_files)) > 0:
        status = 'FAIL'
        for i in failed_files:
            logging.error(f"{i} failed to upload")
    else:
        status = 'SUCCESS'

    sender = email_from
    receivers = email_to

    message = f"""From: Amol Singh <{sender}>
    To: To Person <{receivers}>
    Subject: Backup {status}

    Backup {status}!.
    """
if __name__ == "__main__":
    upload_s3()
    verify_s3()
    clean_s3()
    email_notify()
