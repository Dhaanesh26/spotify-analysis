import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node artists
artists_node1752821017147 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspotifybucket-data-analysis/stagging/artists.csv"], "recurse": True}, transformation_ctx="artists_node1752821017147")

# Script generated for node albums
albums_node1752821060130 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspotifybucket-data-analysis/stagging/albums.csv"], "recurse": True}, transformation_ctx="albums_node1752821060130")

# Script generated for node tracks
tracks_node1752821063636 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://myspotifybucket-data-analysis/stagging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1752821063636")

# Script generated for node Join album & artist
Joinalbumartist_node1752821490216 = Join.apply(frame1=albums_node1752821060130, frame2=artists_node1752821017147, keys1=["artist_id"], keys2=["id"], transformation_ctx="Joinalbumartist_node1752821490216")

# Script generated for node Join with tracks
Joinwithtracks_node1752821604124 = Join.apply(frame1=tracks_node1752821063636, frame2=Joinalbumartist_node1752821490216, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1752821604124")

# Script generated for node Drop Fields
DropFields_node1752821734739 = DropFields.apply(frame=Joinwithtracks_node1752821604124, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1752821734739")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1752821734739, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1752820979665", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1752821803629 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1752821734739, connection_type="s3", format="glueparquet", connection_options={"path": "s3://myspotifybucket-data-analysis/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1752821803629")

job.commit()