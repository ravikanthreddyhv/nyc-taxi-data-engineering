import boto3
from datetime import datetime

redshift = boto3.client(
    "redshift-data",
    region_name="us-east-1"
)

DATABASE = "dev"
WORKGROUP = "default-workgroup"
LINEAGE_TABLE = "public.data_lineage"


def lambda_handler(event, context):
    sql = f"""
        INSERT INTO {LINEAGE_TABLE} (
            pipeline_name,
            pipeline_stage,
            source_layer,
            source_dataset,
            dataset_layer,
            dataset_name,
            transformation_name,
            transformation_type,
            created_by,
            created_at,
            is_active,
            lineage_version
        )
        VALUES (
            '{event["pipeline_name"]}',
            '{event["pipeline_stage"]}',
            '{event["source_layer"]}',
            '{event["source_dataset"]}',
            '{event["target_layer"]}',
            '{event["target_dataset"]}',
            '{event["transformation_name"]}',
            'GLUE_ETL',
            'step_function',
            '{datetime.utcnow()}',
            true,
            1
        );
    """

    response = redshift.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql
    )

    return {
        "status": "LINEAGE_RECORDED",
        "statement_id": response["Id"]
    }
