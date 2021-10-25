Streamlined backup
==================

![GitHub Workflow](https://github.com/chialab/streamlined-backup/actions/workflows/test.yml/badge.svg) [![Codecov coverage](https://codecov.io/gh/chialab/streamlined-backup/branch/main/graph/badge.svg?token=PNQT4539HK)](https://codecov.io/gh/chialab/streamlined-backup)

This tool is a simple, portable single binary that is able to run backup tasks
at a fixed schedule and upload artifacts to a remote destination.

This tool does not rely on temporary files: the upload task expects a command
that outputs its results as stdout, which is then read and uploaded in chunks
to the remote server as it is produced. This makes this tool ideal in cases where
the filesystem is read only (such as containers) or where there is disk pressure.

Finally, you can pass one or more Slack webhook URLs to the tool to be notified
when the backup is complete, or when it fails.

Example configuration
---------------------

Configuration can be either in JSON or TOML format. The binary expects path to
configuration file to be passed using the `--config` command line argument.

The followind example uses TOML:

```toml
[backup_mysql_database]
schedule = "30 4 * * *"
command = ["/bin/sh", "-c", "mysqldump --single-transaction --column-statistics=0 --set-gtid-purged=off my_database | bzip2"]
    [backup_mysql_database.destination]
    type = "s3"
        [backup_mysql_database.destination.s3]
        region = "eu-west-1"
        profile = "example-profile"
        bucket = "example-bucket"
        prefix = "my_database/daily/"
        suffix = "-my_database.sql.bz2"

[my_tar_archive]
schedule = "30 4 * * *"
command = ["tar", "-cvjf-", "/path/to/files"]
    [my_tar_archive.destination]
    type = "s3"
        [my_tar_archive.destination.s3]
        region = "eu-west-1"
        bucket = "example-bucket"
        prefix = "my_tar_archive/daily/"
        suffix = "-my_tar_archive.tar.bz2"
            [my_tar_archive.destination.s3.credentials]
            access_key_id = "AKIAIOSFODNN7EXAMPLE"
            secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```
