CREATE EXTERNAL TABLE IF NOT EXISTS test_db.test_table (
    `COLUMN1` varchar(100),
	`COLUMN2` integer,
	`COLUMN3` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
LOCATION
    's3://test-bucket/test_prefix/'
TBLPROPERTIES (
    'classification' = 'parquet',
	'compressionType' = 'snappy'
)
