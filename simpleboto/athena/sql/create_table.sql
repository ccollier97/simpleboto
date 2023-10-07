CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}{table_name} (
    {column_schema}
){partitioned_by}
ROW FORMAT SERDE
    '{row_format_serde}'
LOCATION
    '{location}'
TBLPROPERTIES (
    {tbl_properties}
)
