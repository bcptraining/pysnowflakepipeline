from snowflake.snowpark.types import StructType, StructField, StringType, LongType

emp_details_avro_cls_V1_0 = StructType(
    [
        StructField("REGISTRATION", StringType(), nullable=False),
        StructField("USER_ID", LongType(), nullable=False),
        StructField("FIRST_NAME", StringType(), nullable=False),
        StructField("LAST_NAME", StringType(), nullable=False),
        StructField("USER_EMAIL", StringType(), nullable=False),
    ]
)
schema_registry = {
    "emp_details_avro_cls": {
        "v1.0": emp_details_avro_cls_V1_0,
    }
}
