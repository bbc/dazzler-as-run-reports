# Purpose

This takes the Media Live as run logs and produces daily exports to an S3 bucket suitable for AWS Athena.

This will create the appropriate table in Athena:
    CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`iplayerdazzlerasruns` (
    `name` varchar(64),
    `start` timestamp,
    `duration` bigint,
    `channel_vpid` varchar(48),
    `item_vpid` varchar(48),
        `end` timestamp
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION 's3://iplayer-dazzler-asruns/daily/'
    TBLPROPERTIES ('classification' = 'parquet');


# Checking
This SQL query shows if a given day has about 24 hours of as-runs.

    select name, (86400 - (sum(duration)/1000000000)) / 60 as total_duration from iplayerdazzlerasruns where start between from_iso8601_timestamp('2024-12-15') and from_iso8601_timestamp('2024-12-16') group by name