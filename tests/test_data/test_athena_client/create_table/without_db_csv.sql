CREATE EXTERNAL TABLE IF NOT EXISTS test_table (
    `COLUMN1` string,
	`COLUMN2` boolean,
	`COLUMN3` integer
)
PARTITIONED BY (
	`COLUMN4` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION
    's3://test-bucket/test_prefix/'
TBLPROPERTIES (
    'classification' = 'csv',
	'projection.enabled' = 'TRUE',
	'projection.COLUMN4.type' = 'enum',
	'projection.COLUMN4.values' = 'VAL1,VAL2'
)
