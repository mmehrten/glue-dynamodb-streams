import decimal
import enum
import json
import sys
from typing import Dict, Optional, Tuple

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from boto3.dynamodb.types import TypeDeserializer
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json, isnull, lit, udf, when
from pyspark.sql.types import *


# Helper methods for dealing with inputs / serialization
class OutputMode(enum.Enum):
    """How to write data, configured in the Glue job arguments."""

    REDSHIFT = enum.auto()
    S3 = enum.auto()


class DecimalEncoder(json.JSONEncoder):
    """Handle JSON encoding of Decimal data (necessary because TypeDeserializer defaults to Decimal for floating point values)."""

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


# Glue boilerplate
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "StreamARN",
        "OutputMode",
        "RedshiftConnectionName",
        "S3OutputPath",
        "RedshiftTableName",
        "RedshiftStagingTableName",
        "RedshiftSchema",
    ],
)
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Constants used throughout the script
TMP_DIR = args["TempDir"]
TABLE_NAME = args["RedshiftTableName"]
STAGING_TABLE_NAME = args["RedshiftStagingTableName"]
SCHEMA = args["RedshiftSchema"]
DESERIALIZER = TypeDeserializer()
OUTPUT_MODE = OutputMode[args["OutputMode"].upper()]

# These actions get run at the beginning of each microbatch, ensuring that a fresh Glue table exists
# for staging data
PREACTIONS = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_NAME} (
    event_id VARCHAR, 
    last_event_name VARCHAR, 
    table_name VARCHAR, 
    approx_creation_timestamp_millis BIGINT, 
    keys VARCHAR, 
    new_image VARCHAR, 
    has_parsing_error BOOLEAN, 
    is_deleted BOOLEAN, 
    raw_payload_size_bytes INT, 
    raw_payload VARCHAR
); 
DROP TABLE IF EXISTS {SCHEMA}.{STAGING_TABLE_NAME};
CREATE TABLE {SCHEMA}.{STAGING_TABLE_NAME} (
    event_id VARCHAR, 
    last_event_name VARCHAR, 
    table_name VARCHAR, 
    approx_creation_timestamp_millis BIGINT, 
    keys VARCHAR, 
    new_image VARCHAR, 
    has_parsing_error BOOLEAN, 
    is_deleted BOOLEAN, 
    raw_payload_size_bytes INT, 
    raw_payload VARCHAR
);
"""

# These actions run at the end of each microbatch, after the data has been loaded into the
# staging table it is merged into the main table
POSTACTIONS = f"""
BEGIN; 
MERGE INTO {SCHEMA}.{TABLE_NAME} USING {SCHEMA}.{STAGING_TABLE_NAME} 
    ON {SCHEMA}.{TABLE_NAME}.keys = {SCHEMA}.{STAGING_TABLE_NAME}.keys 
WHEN MATCHED THEN UPDATE SET keys = {SCHEMA}.{STAGING_TABLE_NAME}.keys  
WHEN NOT MATCHED THEN INSERT VALUES (
    {SCHEMA}.{STAGING_TABLE_NAME}.event_id, 
    {SCHEMA}.{STAGING_TABLE_NAME}.last_event_name, 
    {SCHEMA}.{STAGING_TABLE_NAME}.table_name, 
    {SCHEMA}.{STAGING_TABLE_NAME}.approx_creation_timestamp_millis, 
    {SCHEMA}.{STAGING_TABLE_NAME}.keys, 
    {SCHEMA}.{STAGING_TABLE_NAME}.new_image, 
    {SCHEMA}.{STAGING_TABLE_NAME}.has_parsing_error, 
    {SCHEMA}.{STAGING_TABLE_NAME}.is_deleted, 
    {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload_size_bytes, 
    {SCHEMA}.{STAGING_TABLE_NAME}.raw_payload
); 
DROP TABLE {SCHEMA}.{STAGING_TABLE_NAME}; 
END
"""

# Helper methods to convert DynamoDB syntax data into true JSON data
# See the raw and converted data examples in ./parsed-data/
def _ddb_to_json(data: Dict, prop: str) -> Optional[str]:
    """Convert DynamoDB encoded data into normal JSON.

    :param data: A mapping of {"key": {dynamo db encoded data}}
    :param prop: The key to convert from the input data (e.g. Keys or NewImage from DynamoDB Streams)
    """
    if prop not in data:
        return None
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
            data.get("SizeBytes"),
            None,
        )
    except Exception as e:
        return (
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
            StructField("SizeBytes", IntegerType(), True),
            StructField("error", StringType(), True),
        ]
    ),
)

# Connection options for S3 and Redshift modes
REDSHIFT_CONNECTION_OPTS = {
    "postactions": POSTACTIONS,
    "redshiftTmpDir": TMP_DIR,
    "useConnectionProperties": "true",
    "dbtable": "tmp",
    "connectionName": args["RedshiftConnectionName"],
    "preactions": PREACTIONS,
}
S3_CONNECTION_OPTS = {"path": args["S3OutputPath"]}
KINESIS_CONNECTION_OPTS = {
    "typeOfData": "kinesis",
    "streamARN": args["StreamARN"],
    "classification": "json",
    "startingPosition": "earliest",
    "inferSchema": "true",
}


# Actual Spark Streaming method used to process each microbatch
def processBatch(data_frame, batchId):
    # If this microbatch is empty, do nothing
    if data_frame.count() == 0:
        return

    data_frame = (
        # First, parse the input JSON data from the Kinesis stream
        # into a top-level schema. This uses a magic Glue column named
        # "$json$data_infer_schema$_temporary$" which is the input column
        # when using inferred schemas from JSON data. Its type is a STRING,
        # so we simply do a from_json to convert it into a true structure
        data_frame.withColumn(
            "data",
            from_json(
                col("$json$data_infer_schema$_temporary$"),
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
        .select(
            "data.*", col("$json$data_infer_schema$_temporary$").alias("raw_payload")
        )
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
    )

    # Now that we've processed the data, we can write it out to our target
    # destination (either S3 or Redshift)
    kinesis_microbatch_node = DynamicFrame.fromDF(
        data_frame, glueContext, "from_data_frame"
    )
    if OUTPUT_MODE == OutputMode.REDSHIFT:
        glueContext.write_dynamic_frame.from_options(
            frame=kinesis_microbatch_node,
            connection_type="redshift",
            connection_options=REDSHIFT_CONNECTION_OPTS,
            transformation_ctx="redshift_tfx_node",
        )
    elif OUTPUT_MODE == OutputMode.S3:
        glueContext.write_dynamic_frame.from_options(
            frame=kinesis_microbatch_node,
            connection_type="s3",
            connection_options=S3_CONNECTION_OPTS,
            transformation_ctx="redshift_tfx_node",
            format="json",
        )
    else:
        raise NotImplementedError(f"No output mode: {OUTPUT_MODE}")


# Finally, run the job!
kinesis_gdf = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options=KINESIS_CONNECTION_OPTS,
    transformation_ctx="kinesis_tfx_node",
)
glueContext.forEachBatch(
    frame=kinesis_gdf,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()