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
    current_timestamp,
    from_json,
    input_file_name,
    isnull,
    length,
    lit,
    struct,
    to_json,
    udf,
    unix_timestamp,
)
from pyspark.sql.types import StringType, StructField, StructType


class DecimalEncoder(json.JSONEncoder):
    """Handle JSON encoding of Decimal data (necessary because TypeDeserializer defaults to Decimal for floating point values)."""

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def get_glue_env_var(key, default=None):
    arg_key = key if key != "JOB_RUN_ID" else "JOB_NAME"
    if f"--{key}" in sys.argv:
        return getResolvedOptions(sys.argv, [arg_key])[key]
    else:
        return default


# Glue boilerplate
# Constants used throughout the script
JOB_NAME = get_glue_env_var("JOB_NAME")
TMP_DIR = get_glue_env_var("TempDir")
TABLE_NAME = get_glue_env_var("RedshiftTableName")
STAGING_TABLE_NAME = get_glue_env_var("RedshiftStagingTableName")
SCHEMA = get_glue_env_var("RedshiftSchema")
REDSHIFT_CONNECTION_NAME = get_glue_env_var("RedshiftConnectionName")
S3_DDB_EXPORT_PATH = get_glue_env_var("S3DynamoDbExportPath")
S3_DATA_PATH = f"{S3_DDB_EXPORT_PATH}/data/"
S3_JOIN_PATH = get_glue_env_var("S3JoinPath")
JOB_RUN_ID = get_glue_env_var("JOB_RUN_ID")
DDB_TABLE_NAME = get_glue_env_var("DynamoDbTableName")
DDB_TABLE_KEYS = get_glue_env_var("DynamoDbTableKeys")
REDSHIFT_IAM_ROLE = get_glue_env_var("RedshiftIAMRole")

assert all(
    (
        DDB_TABLE_KEYS,
        DDB_TABLE_NAME,
        S3_DDB_EXPORT_PATH,
        S3_JOIN_PATH,
        REDSHIFT_CONNECTION_NAME,
        SCHEMA,
        STAGING_TABLE_NAME,
        TABLE_NAME,
    )
), "Parameters RedshiftTableName, RedshiftStagingTableName, RedshiftSchema, RedshiftConnectionName, S3DynamoDbExportPath, DynamoDbTableName, DynamoDbTableKeys, S3JoinPath are required."
# Clean up user input to avoid trailing spaces
DDB_TABLE_KEYS = [i.strip() for i in DDB_TABLE_KEYS.split(",") if i.strip()]

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
def _ddb_to_json(data: Dict, prop: str) -> Optional[Dict]:
    """Convert DynamoDB encoded data into normal JSON.

    :param data: A mapping of {"key": {dynamo db encoded data}}
    :param prop: The key to convert from the input data (e.g. Keys or NewImage from DynamoDB Streams)
    """
    if prop not in data:
        return {}
    return DESERIALIZER.deserialize({"M": data[prop]})


def parse_dynamodb(dynamodb_json_string: str) -> Tuple:
    """Parse the "dynamodb" key from a DynamoDB Streams message into a Spark struct with JSON encoded keys / image.

    Converts from DynamoDB record encoding to normal JSON encoding.

    Note: If errors are encountered in parsing, the "error" property will be non-null.
    """
    try:
        data = json.loads(dynamodb_json_string)
        record = _ddb_to_json(data, "Item")
        # Create the Key definition manually using the configured keys in the Job parameters
        key = {k: record[k] for k in DDB_TABLE_KEYS}
        return (
            json.dumps(key, cls=DecimalEncoder),
            json.dumps(record, cls=DecimalEncoder),
            None,
        )
    except Exception as e:
        return (
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
            StructField("Keys", StringType(), True),
            StructField("NewImage", StringType(), True),
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


data_frame = (
    spark.read.format("json")
    .load(S3_DATA_PATH)
    .withColumn("raw_payload", to_json(struct("*")))
    .select("raw_payload")
    # Use the UDF to parse the dynamodb sub-payload into human-readable JSON
    .withColumn("dynamodb_decoded", PARSE_DYNAMODB_UDF("raw_payload"))
    # Reformat to the output structure we want
    .select(
        lit(JOB_RUN_ID).alias("event_id"),
        # Use BULK_LOAD to differ from streaming insert/remove/update operations
        lit("BULK_LOAD").alias("last_event_name"),
        lit(DDB_TABLE_NAME).alias("table_name"),
        # unix_timestamp is in seconds, convert to millis
        (1000 * unix_timestamp(current_timestamp())).alias(
            "approx_creation_timestamp_millis"
        ),
        col("dynamodb_decoded.Keys").alias("keys"),
        col("dynamodb_decoded.NewImage").alias("new_image"),
        lit("{}").alias("old_image"),
        # If there was a parsing error when converting the DynamoDB sub-payload,
        # we can indicate it here
        col("dynamodb_decoded.error").alias("error"),
        (~isnull("dynamodb_decoded.error")).alias("has_parsing_error"),
        # If the event is a REMOVE event, we can mark this row as deleted as it will no longer
        # be used and the new_image column will be NULL
        lit(False).alias("is_deleted"),
        # Store the original raw data as well in case it's useful for troubleshooting
        (4 * length("raw_payload")).alias("raw_payload_size_bytes"),  # Estimate
        col("raw_payload"),
    )
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
    data_frame.join(other_s3_df, other_s3_df.s3_uri == data_frame.ddb.s3_uri, "left")
    .na.fill("{}", ["raw_s3_payload", "new_image", "old_image"])
    .drop("ddb")
)
s3_microbatch_node = DynamicFrame.fromDF(ddb_with_s3, glueContext, "from_data_frame")
glueContext.write_dynamic_frame.from_options(
    frame=s3_microbatch_node,
    connection_type="redshift",
    connection_options=REDSHIFT_CONNECTION_OPTS,
    transformation_ctx="s3_tfx_node",
)

job.commit()
