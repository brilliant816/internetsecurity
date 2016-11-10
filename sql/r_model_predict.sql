CREATE EXTERNAL TABLE r_model_predict
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/user/root/mofan/r_model_predict'
TBLPROPERTIES ('avro.schema.literal'='{
  "type": "record",
  "name": "RModelParam",
  "namespace": "schema",
  "fields": [
    {
      "name": "table_name",
      "type": [ "null", "string" ],
      "default": null
    },
    {
      "name": "param_key",
      "type": [ "null", "string" ],
      "default": null
    }, {
      "name": "param_value",
      "type": [ "null", "string" ],
      "default": null
    }
  ]
}')