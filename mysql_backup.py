#!/usr/bin/env python
"""
MySQL To S3 Backup Script

Does a MySQL dump, gzips the dump, and uploads it to Amazon S3.

 -a, --access-key  Your AWS Access Key
 -s, --secret-key  Your AWS Secret Key
 -b, --bucket      The S3 bucket to upload backups to
 -d, --dir         The local directory to store backups in (/var/lib/backups)
 -u, --user        MySQL user to use for mysqldump
 -p, --pass        MySQL password to use for mysqldump
 -t, --age         Maximum number of days to keep backups for (15)
"""

import datetime, getopt, subprocess, sys, time, tinys3

access_key = ""
secret_key = ""
bucket_name = ""
local_dir = "/var/lib/backups"
mysql_user = ""
mysql_pass = ""
prune_after = 15
backup_name = "dump-%s.sql.gz" % datetime.datetime.today().strftime("%Y-%m-%d")

# Command line options
opts, args = getopt.getopt(sys.argv[1:], 'a:s:b:d:u:p:t:', ['access-key=', 'secret-key=', 'bucket=', 'dir=', 'user=', 'pass=', 'age='])

 # Get command line arguments/options
for opt, value in opts:
    if opt in ('-a', '--access-key'):
        access_key = value
    elif opt in ('-s', '--secret-key'):
        secret_key = value
    elif opt in ('-b', '--bucket'):
        bucket_name = value
    elif opt in ('-d', '--dir'):
        local_dir = value
    elif opt in ('-u', '--user'):
        mysql_user = value
    elif opt in ('-p', '--pass'):
        mysql_pass = value
    elif opt in ('-t', '--age'):
        prune_after = value

# Generate a backup using mysqldump
subprocess.call("mysqldump --user='%s' --password='%s' --all-databases 2>/dev/null | gzip > %s/%s" % (mysql_user, mysql_pass, local_dir, backup_name), shell=True)
print "Dumped all databases to %s/%s" % (local_dir, backup_name)

# Connect to S3
conn = tinys3.Connection(access_key, secret_key)

# Upload our backup to S3
f = open(local_dir + '/' + backup_name, 'rb')
conn.upload('mysql/' + backup_name, f, bucket_name, public=False)
print 'Uploaded to https://s3.amazonaws.com/%s/mysql/%s' % (bucket_name, backup_name)

# Get a list of all keys in this bucket
backups = conn.list('mysql/', bucket_name)

# Iterate over each key and prune ones older than 15 days
for backup in backups:
    try:
        date = backup['key'].replace('mysql/dump-', '').replace('.sql.gz', '')
        date = time.mktime(time.strptime(date, '%Y-%m-%d'))
    except:
        date = now - (86400 * prune_after) - 1

    # file is 15 days old?
    if (date + (86400 * prune_after)) < time.time():
        conn.delete(backup['key'], bucket_name)
        print 'Pruned old backup: %s' % backup['key']
