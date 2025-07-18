from snowflake.snowpark.types import StructType, StructField, StringType, LongType

emp_details_avro_cls = StructType(
    [
        StructField("REGISTRTION", StringType(), nullable=False),
        StructField("USER_ID", LongType(), nullable=False),
        StructField("FIRST_NAME", StringType(), nullable=False),
        StructField("LAST_NAME", StringType(), nullable=False),
        StructField("USER_EMAIL", StringType(), nullable=False),
    ]
)
