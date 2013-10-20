Pushtos3 is a simple script to backup your mysql databases and files then upload to s3. You can upload to amazon s3 or other cloud that compatible with s3 API. Pushtos3 can backup periodically between daily, weekly and monthly. For daily backup, script with keep last 4 backup on s3, backup file for last 4 day, then backup weekly every monday, and monthly backup on the 1st every month.

## Requirement
* Python 2.6 or newer
* Boto (install with pip install boto)
