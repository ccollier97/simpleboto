CREATE EXTERNAL TABLE IF NOT EXISTS test_table (
    `COLUMN1` string,
	`COLUMN2` boolean,
	`COLUMN3` integer
)
PARTITIONED BY (
	`COLUMN4` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
LOCATION
    's3://test-bucket/test_prefix/'
TBLPROPERTIES (
    'classification' = 'parquet',
	'compressionType' = 'snappy',
	'projection.enabled' = 'TRUE',
	'projection.COLUMN4.type' = 'enum',
	'projection.COLUMN4.values' = 'VAL1,VAL2'
)
