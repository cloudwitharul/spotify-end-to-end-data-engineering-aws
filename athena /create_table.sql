CREATE EXTERNAL TABLE IF NOT EXISTS spotify.datawarehouse (
    followers int,
    track_id string,
    artist_popularity int,
    artist_id string,
    album_id string,
    name string
)
STORED AS PARQUET
LOCATION 's3://project-spotify-datewithdata-ay/datawarehouse/';
