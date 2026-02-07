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

# Script generated for node album
album_node1769609283491 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-ay/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1769609283491")

# Script generated for node artist
artist_node1769609286022 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-ay/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1769609286022")

# Script generated for node tracks
tracks_node1769609287451 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-datewithdata-ay/staging/track.csv"], "recurse": True}, transformation_ctx="tracks_node1769609287451")

# Script generated for node Join Album & Artist
JoinAlbumArtist_node1769609476489 = Join.apply(frame1=album_node1769609283491, frame2=artist_node1769609286022, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1769609476489")

# Script generated for node Join with tracks
Joinwithtracks_node1769609577967 = Join.apply(frame1=tracks_node1769609287451, frame2=JoinAlbumArtist_node1769609476489, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1769609577967")

# Script generated for node Drop Fields
DropFields_node1769609709126 = DropFields.apply(frame=Joinwithtracks_node1769609577967, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1769609709126")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1769609709126, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769609272017", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1769609811759 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1769609709126, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-datewithdata-ay/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1769609811759")

job.commit()
