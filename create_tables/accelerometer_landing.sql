CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` decimal(38,18) COMMENT 'from deserializer', 
  `y` decimal(38,18) COMMENT 'from deserializer', 
  `z` decimal(38,18) COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://accelerometer-landing-jzanar1/'
TBLPROPERTIES (
  'classification'='json')