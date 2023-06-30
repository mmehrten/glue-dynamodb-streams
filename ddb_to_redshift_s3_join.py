import decimal
import json
import sys
from typing import Dict, Optional, Tuple

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from boto3.dynamodb.types import TypeDeserializer
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    from_json,
    input_file_name,
    isnull,
    lit,
    row_number,
    struct,
    to_json,
    udf,
    when,
)
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window


class DecimalEncoder(json.JSONEncoder):
    """Handle JSON encoding of Decimal data (necessary because TypeDeserializer defaults to Decimal for floating point values)."""

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def get_glue_env_var(key, default=None):
    if f"--{key}" in sys.argv:
        return getResolvedOptions(sys.argv, [key])[key]
    else:
        return default


# Glue boilerplate
# Constants used throughout the script
JOB_NAME = get_glue_env_var("JOB_NAME")
TMP_DIR = get_glue_env_var("TempDir")
TABLE_NAME = get_glue_env_var("RedshiftTableName")
STAGING_TABLE_NAME = get_glue_env_var("RedshiftStagingTableName")
SCHEMA = get_glue_env_var("RedshiftSchema")
KINESIS_STREAM = get_glue_env_var("StreamARN")
KINESIS_IAM_ROLE = get_glue_env_var("KinesisRoleARN")
REDSHIFT_CONNECTION_NAME = get_glue_env_var("RedshiftConnectionName")
S3_JOIN_PATH = get_glue_env_var("S3JoinPath")
REDSHIFT_IAM_ROLE = get_glue_env_var("RedshiftIAMRole")
DESERIALIZER = TypeDeserializer()
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, getResolvedOptions(sys.argv, []))


# These actions get run at the beginning of each microbatch, ensuring that a fresh Glue table exists
# for staging data
# Note: The SQL for pre/post actions can't include comments (as they must be single-line and newlines are removed below)
PREACTIONS = f"""
BEGIN;
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
    event_id VARCHAR,
    last_event_name VARCHAR,
    table_name VARCHAR,
    approx_creation_timestamp_millis BIGINT,
    keys VARCHAR,
    new_image SUPER,
    old_image SUPER,
    error VARCHAR,
    has_parsing_error BOOLEAN,
    is_deleted BOOLEAN,
    raw_payload_size_bytes INT,
    raw_payload VARCHAR(max),
    s3_uri VARCHAR,
    raw_s3_payload SUPER
);
DROP TABLE IF EXISTS {SCHEMA}.{STAGING_TABLE_NAME};
CREATE TABLE {SCHEMA}.{STAGING_TABLE_NAME} (
    event_id VARCHAR,
    last_event_name VARCHAR,
    table_name VARCHAR,
    approx_creation_timestamp_millis BIGINT,
    keys VARCHAR,
    new_image SUPER,
    old_image SUPER,
    error VARCHAR,
    has_parsing_error BOOLEAN,
    is_deleted BOOLEAN,
    raw_payload_size_bytes INT,
    raw_payload VARCHAR(max),
    s3_uri VARCHAR,
    raw_s3_payload SUPER
);
END;
"""

# These actions run at the end of each microbatch, after the data has been loaded into the
# staging table it is merged into the main table
POSTACTIONS = f"""
BEGIN;
MERGE INTO {SCHEMA}.{TABLE_NAME} USING {SCHEMA}.{STAGING_TABLE_NAME}
    ON {SCHEMA}.{TABLE_NAME}.keys = {SCHEMA}.{STAGING_TABLE_NAME}.keys
WHEN MATCHED THEN UPDATE SET 
    event_id = {SCHEMA}.{STAGING_TABLE_NAME}.event_id,
    last_event_name = {SCHEMA}.{STAGING_TABLE_NAME}.last_event_name,
    table_name = {SCHEMA}.{STAGING_TABLE_NAME}.table_name,
    approx_creation_timestamp_millis = {SCHEMA}.{STAGING_TABLE_NAME}.approx_creation_timestamp_millis,
    new_image = {SCHEMA}.{STAGING_TABLE_NAME}.new_image,
    old_image = {SCHEMA}.{STAGING_TABLE_NAME}.old_image,
    error = {SCHEMA}.{STAGING_TABLE_NAME}.error,
    has_parsing_error = {SCHEMA}.{STAGING_TABLE_NAME}.has_parsing_error,
    is_deleted = {SCHEMA}.{STAGING_TABLE_NAME}.is_deleted,
    raw_payload_size_bytes = {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload_size_bytes,
    raw_payload = {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload,
    s3_uri = {SCHEMA}.{STAGING_TABLE_NAME}.s3_uri,
    raw_s3_payload = {SCHEMA}.{STAGING_TABLE_NAME}.raw_s3_payload

WHEN NOT MATCHED THEN INSERT VALUES (
    {SCHEMA}.{STAGING_TABLE_NAME}.event_id,
    {SCHEMA}.{STAGING_TABLE_NAME}.last_event_name,
    {SCHEMA}.{STAGING_TABLE_NAME}.table_name,
    {SCHEMA}.{STAGING_TABLE_NAME}.approx_creation_timestamp_millis,
    {SCHEMA}.{STAGING_TABLE_NAME}.keys,
    {SCHEMA}.{STAGING_TABLE_NAME}.new_image,
    {SCHEMA}.{STAGING_TABLE_NAME}.old_image,
    {SCHEMA}.{STAGING_TABLE_NAME}.error,
    {SCHEMA}.{STAGING_TABLE_NAME}.has_parsing_error,
    {SCHEMA}.{STAGING_TABLE_NAME}.is_deleted,
    {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload_size_bytes,
    {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload,
    {SCHEMA}.{STAGING_TABLE_NAME}.s3_uri,
    {SCHEMA}.{STAGING_TABLE_NAME}.raw_s3_payload
);
DROP TABLE {SCHEMA}.{STAGING_TABLE_NAME};
END;
"""

# Helper methods to convert DynamoDB syntax data into true JSON data
# See the raw and converted data examples in ./parsed-data/
def _ddb_to_json(data: Dict, prop: str) -> Optional[str]:
    """Convert DynamoDB encoded data into normal JSON.

    :param data: A mapping of {"key": {dynamo db encoded data}}
    :param prop: The key to convert from the input data (e.g. Keys or NewImage from DynamoDB Streams)
    """
    if prop not in data:
        return "{}"
    return json.dumps(DESERIALIZER.deserialize({"M": data[prop]}), cls=DecimalEncoder)


def parse_dynamodb(dynamodb_json_string: str) -> Tuple:
    """Parse the "dynamodb" key from a DynamoDB Streams message into a Spark struct with JSON encoded keys / image.

    Converts from DynamoDB record encoding to normal JSON encoding.

    Note: If errors are encountered in parsing, the "error" property will be non-null.
    """
    try:
        data = json.loads(dynamodb_json_string)
        return (
            data.get("ApproximateCreationDateTime"),
            _ddb_to_json(data, "Keys"),
            _ddb_to_json(data, "NewImage"),
            _ddb_to_json(data, "OldImage"),
            data.get("SizeBytes"),
            None,
        )
    except Exception as e:
        return (
            None,
            None,
            None,
            None,
            None,
            json.dumps(
                {"error": str(e), "data": dynamodb_json_string}, cls=DecimalEncoder
            ),
        )


PARSE_DYNAMODB_UDF = udf(
    parse_dynamodb,
    StructType(
        [
            StructField("ApproximateCreationDateTime", LongType(), True),
            StructField("Keys", StringType(), True),
            StructField("NewImage", StringType(), True),
            StructField("OldImage", StringType(), True),
            StructField("SizeBytes", IntegerType(), True),
            StructField("error", StringType(), True),
        ]
    ),
)


def _trim_actions(actions: str) -> str:
    """Remove newlines and trim whitespace for a Redshift action."""
    return " ".join(i.strip() for i in actions.split("\n") if i.strip())


# Connection options for S3 and Redshift modes
REDSHIFT_CONNECTION_OPTS = {
    # Note: Redshift preactions / postactions can't contain newline characters:
    # https://repost.aws/knowledge-center/sql-commands-redshift-glue-job
    "postactions": _trim_actions(POSTACTIONS),
    "redshiftTmpDir": TMP_DIR,
    "useConnectionProperties": "true",
    "dbtable": f"{SCHEMA}.{STAGING_TABLE_NAME}",
    "connectionName": REDSHIFT_CONNECTION_NAME,
    "preactions": _trim_actions(PREACTIONS),
    "aws_iam_role": REDSHIFT_IAM_ROLE,
}
# NOTE: For cross-account Kinesis streams, this needs the role ARN to assume
KINESIS_CONNECTION_OPTS = {
    "typeOfData": "kinesis",
    "streamARN": KINESIS_STREAM,
    "classification": "json",
    "startingPosition": "earliest",
    "inferSchema": "true",
}
# Allow setting cross-account role
if KINESIS_IAM_ROLE:
    KINESIS_CONNECTION_OPTS["awsSTSRoleARN"] = KINESIS_IAM_ROLE


# Actual Spark Streaming method used to process each microbatch
def processBatch(data_frame, batchId):
    # If this microbatch is empty, do nothing
    if data_frame.count() == 0:
        return
    # This uses a magic Glue column named
    # "$json$data_infer_schema$_temporary$" which is the input column
    # when using inferred schemas from JSON data. Its type is a STRING,
    # so we simply do a from_json to convert it into a true structure. In some
    # Glue releases this is "$json$data_infer_schema$.temporary$", so we allow both forms
    assert (
        data_frame.schema and len(data_frame.schema) == 1
    ), f"Invalid input schema: {data_frame.schema}"
    root_column = col(f"`{data_frame.schema[0].name}`")
    data_frame = (
        # First, parse the input JSON data from the Kinesis stream
        # into a top-level schema.
        data_frame.withColumn(
            "data",
            from_json(
                root_column,
                # This struct matches the Kinesis Stream payload format
                StructType(
                    [
                        StructField("awsRegion", StringType(), False),
                        StructField("eventID", StringType(), False),
                        StructField("eventName", StringType(), False),
                        StructField("userIdentity", StringType(), False),
                        StructField("recordFormat", StringType(), False),
                        StructField("tableName", StringType(), False),
                        StructField("dynamodb", StringType(), False),
                        StructField("eventSource", StringType(), False),
                    ]
                ),
            ),
        )
        # Unnest the data struct we created, and rename the magic Glue column name to a more
        # sane output column "raw_payload"
        .select("data.*", root_column.alias("raw_payload"))
        # Use the UDF to parse the dynamodb sub-payload into human-readable JSON
        .withColumn("dynamodb_decoded", PARSE_DYNAMODB_UDF("dynamodb"))
        # Reformat to the output structure we want
        .select(
            col("eventID").alias("event_id"),
            col("eventName").alias("last_event_name"),
            col("tableName").alias("table_name"),
            col("dynamodb_decoded.ApproximateCreationDateTime").alias(
                "approx_creation_timestamp_millis"
            ),
            col("dynamodb_decoded.Keys").alias("keys"),
            col("dynamodb_decoded.NewImage").alias("new_image"),
            col("dynamodb_decoded.OldImage").alias("old_image"),
            # If there was a parsing error when converting the DynamoDB sub-payload,
            # we can indicate it here
            col("dynamodb_decoded.error").alias("error"),
            (~isnull("dynamodb_decoded.error")).alias("has_parsing_error"),
            # If the event is a REMOVE event, we can mark this row as deleted as it will no longer
            # be used and the new_image column will be NULL
            (when(col("eventName") == "REMOVE", True).otherwise(lit(False))).alias(
                "is_deleted"
            ),
            # Store the original raw data as well in case it's useful for troubleshooting
            col("dynamodb_decoded.SizeBytes").alias("raw_payload_size_bytes"),
            col("raw_payload"),
        )
        # Select only the latest unique row from this microbatch for each key
        .withColumn(
            "sequence_id",
            row_number().over(
                Window.partitionBy("keys").orderBy(
                    col("approx_creation_timestamp_millis").desc()
                )
            ),
        )
        .filter("sequence_id == 1")
        .drop("sequence_id")
    )

    other_s3_df = (
        spark.read.format("json")
        .load(S3_JOIN_PATH)
        .withColumn("raw_s3_payload", to_json(struct("*")))
        .withColumn("s3_uri", input_file_name())
        .select("s3_uri", "raw_s3_payload")
    )
    data_frame = data_frame.withColumn("ddb", from_json("new_image", "s3_uri STRING"))
    ddb_with_s3 = (
        data_frame.join(
            other_s3_df, other_s3_df.s3_uri == data_frame.ddb.s3_uri, "left"
        )
        .drop("ddb")
        .na.fill("{}", ["raw_s3_payload", "new_image", "old_image"])
    )
    s3_microbatch_node = DynamicFrame.fromDF(
        ddb_with_s3, glueContext, "from_data_frame"
    )
    glueContext.write_dynamic_frame.from_options(
        frame=s3_microbatch_node,
        connection_type="redshift",
        connection_options=REDSHIFT_CONNECTION_OPTS,
        transformation_ctx="s3_tfx_node",
    )


# Finally, run the job!
kinesis_df = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options=KINESIS_CONNECTION_OPTS,
    transformation_ctx="kinesis_tfx_node",
)
glueContext.forEachBatch(
    frame=kinesis_df,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": TMP_DIR + "/" + JOB_NAME + "/checkpoint/",
    },
)
job.commit()
