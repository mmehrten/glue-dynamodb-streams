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


class OutputMode(enum.Enum):
    """How to write data."""

    REDSHIFT = enum.auto()
    S3 = enum.auto()


class DecimalEncoder(json.JSONEncoder):
    """Handle JSON encoding of Decimal data (necessary because Spark defaults to Decimal for floating point values)."""

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


class DynamoDBSchema:
    """Class for handling DynamoDB Schema Interactions.

    Example DDB Schema:

    .. ::
        {
            "awsRegion": "us-gov-west-1",
            "eventID": "11bb3997-fbed-4f44-91b5-51717fa12564",
            "eventName": "INSERT",
            "userIdentity": None,
            "recordFormat": "application/json",
            "tableName": "streamer",
            "dynamodb": {
                "ApproximateCreationDateTime": 1683915742954,
                "Keys": {"column1": {"S": "12058"}},
                "NewImage": {
                    "column1": {"S": "12058"},
                    "column2": {
                        "L": [
                            {
                                "M": {
                                    "column5": {"M": {"column6": {"N": "3333"}}},
                                    "column4": {"N": "3333"},
                                    "column3": {"S": "3333"},
                                }
                            },
                            {
                                "M": {
                                    "column5": {"M": {"column6": {"N": "3333"}}},
                                    "column4": {"S": "99D8C93B-90C3-4630-A58E-84484D2DC8E6"},
                                    "column3": {"S": "3333"},
                                }
                            },
                        ]
                    },
                },
                "SizeBytes": 165,
            },
            "eventSource": "aws:dynamodb",
        }

    Example parsed column schema:

    .. ::
        {
            "event_id": "VARCHAR",
            "event_name": "VARCHAR",
            "table_name": "VARCHAR",
            "approx_creation_timestamp_millis": "BIGINT",
            "keys_json": "VARCHAR",
            "new_image_json": "VARCHAR",
            "size_bytes": "INT",
        }
    """

    INFER_SCHEMA_COLUMN = "$json$data_infer_schema$_temporary$"
    PARSED_COLUMNS = {
        "event_id": "VARCHAR",
        "last_event_name": "VARCHAR",
        "table_name": "VARCHAR",
        "approx_creation_timestamp_millis": "BIGINT",
        "keys": "VARCHAR",
        "new_image": "VARCHAR",
        "has_parsing_error": "BOOLEAN",
        "is_deleted": "BOOLEAN",
        "raw_size_bytes": "INT",
        "raw": "VARCHAR",
    }
    KEY_COLUMN = "keys"
    STRUCT_SCHEMA = StructType(
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
    )
    NESTED_STRUCT_SCHEMA = StructType(
        [
            StructField("ApproximateCreationDateTime", LongType(), True),
            StructField("Keys", StringType(), True),
            StructField("NewImage", StringType(), True),
            StructField("SizeBytes", IntegerType(), True),
            StructField("error", StringType(), True),
        ]
    )

    def __init__(
        self,
        redshift_tmp_dir: str,
        table_name: str = "data",
        staging_table_name: str = "tmp",
        schema: str = "public",
    ):
        self.redshift_tmp_dir = redshift_tmp_dir
        self.table_name = table_name
        self.schema = schema
        self.staging_table_name = staging_table_name
        self.deserializer = TypeDeserializer()

    @property
    def preactions(self) -> str:
        """Create staging and live tables for ingest data in Redshift as a pre-action for microbatches."""
        schema = ", ".join(f"{k} {v}" for k, v in self.PARSED_COLUMNS.items())
        return f"""
CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} ({schema}); 
DROP TABLE IF EXISTS {self.schema}.{self.staging_table_name};
CREATE TABLE {self.schema}.{self.staging_table_name} ({schema});
"""

    @property
    def postactions(self) -> str:
        """Merge and clean up up staging as a post-action for microbatches."""
        columns = ", ".join(
            f"{self.staging_table_name}.{col}" for col in self.PARSED_COLUMNS
        )
        return f"""
BEGIN; 
MERGE INTO {self.schema}.{self.table_name} USING {self.schema}.{self.staging_table_name} ON {self.table_name}.{self.KEY_COLUMN} = {self.staging_table_name}.{self.KEY_COLUMN} 
WHEN MATCHED THEN UPDATE SET {self.KEY_COLUMN} = {self.staging_table_name}.{self.KEY_COLUMN}  
WHEN NOT MATCHED THEN INSERT VALUES ({columns}); 
DROP TABLE {self.schema}.{self.staging_table_name}; 
END"""

    def _ddb_to_json(self, data: Dict, prop: str) -> Optional[str]:
        """Convert DynamoDB encoded data into normal JSON.

        :param data: A mapping of {"key": {dynamo db encoded data}}
        :param prop: The key to convert from the input data (e.g. Keys or NewImage from DynamoDB Streams)
        """
        if prop not in data:
            return None
        return json.dumps(
            self.deserializer.deserialize({"M": data[prop]}), cls=DecimalEncoder
        )

    def parse_dynamodb(self, dynamodb_json_string: str) -> Tuple:
        """Parse the "dynamodb" key from a DynamoDB Streams message into a Spark struct with JSON encoded keys / image.

        Converts from DynamoDB record encoding to normal JSON encoding.

        Note: If errors are encountered in parsing, the "error" property will be non-null.
        """
        try:
            data = json.loads(dynamodb_json_string)
            return (
                data.get("ApproximateCreationDateTime"),
                self._ddb_to_json(data, "Keys"),
                self._ddb_to_json(data, "NewImage"),
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


def processBatch(data_frame, batchId):
    if data_frame.count() == 0:
        return

    data_frame = (
        data_frame.withColumn(
            "data",
            from_json(
                col(DDB_SCHEMA_HANDLER.INFER_SCHEMA_COLUMN),
                DDB_SCHEMA_HANDLER.STRUCT_SCHEMA,
            ),
        )
        .select("data.*", col(DDB_SCHEMA_HANDLER.INFER_SCHEMA_COLUMN).alias("raw"))
        .withColumn("dynamodb_decoded", PARSE_DYNAMODB_UDF("dynamodb"))
        .select(
            col("eventID").alias("event_id"),
            col("eventName").alias("last_event_name"),
            col("tableName").alias("table_name"),
            col("dynamodb_decoded.ApproximateCreationDateTime").alias(
                "approx_creation_timestamp_millis"
            ),
            col("dynamodb_decoded.Keys").alias("keys"),
            col("dynamodb_decoded.NewImage").alias("new_image"),
            col("dynamodb_decoded.error").alias("error"),
            (~isnull("dynamodb_decoded.error")).alias("has_parsing_error"),
            (when(col("eventName") == "REMOVE", True).otherwise(lit(False))).alias(
                "is_deleted"
            ),
            col("dynamodb_decoded.SizeBytes").alias("raw_size_bytes"),
            col("raw"),
        )
    )
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


if __name__ == "__main__":
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

    DDB_SCHEMA_HANDLER = DynamoDBSchema(
        redshift_tmp_dir=args["TempDir"],
        table_name=args["RedshiftTableName"],
        staging_table_name=args["RedshiftStagingTableName"],
        schema=args["RedshiftSchema"],
    )
    PARSE_DYNAMODB_UDF = udf(
        DDB_SCHEMA_HANDLER.parse_dynamodb, DDB_SCHEMA_HANDLER.NESTED_STRUCT_SCHEMA
    )
    REDSHIFT_CONNECTION_OPTS = {
        "postactions": DDB_SCHEMA_HANDLER.postactions,
        "redshiftTmpDir": DDB_SCHEMA_HANDLER.redshift_tmp_dir,
        "useConnectionProperties": "true",
        "dbtable": "tmp",
        "connectionName": args["RedshiftConnectionName"],
        "preactions": DDB_SCHEMA_HANDLER.preactions,
    }
    S3_CONNECTION_OPTS = {"path": args["S3OutputPath"]}
    kinesis_connection_opts = {
        "typeOfData": "kinesis",
        "streamARN": args["StreamARN"],
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true",
    }

    OUTPUT_MODE = OutputMode[args["OutputMode"].upper()]

    kinesis_gdf = glueContext.create_data_frame.from_options(
        connection_type="kinesis",
        connection_options=kinesis_connection_opts,
        transformation_ctx="kinesis_tfx_node",
    )
    glueContext.forEachBatch(
        frame=kinesis_gdf,
        batch_function=processBatch,
        options={
            "windowSize": "100 seconds",
            "checkpointLocation": args["TempDir"]
            + "/"
            + args["JOB_NAME"]
            + "/checkpoint/",
        },
    )
    job.commit()
