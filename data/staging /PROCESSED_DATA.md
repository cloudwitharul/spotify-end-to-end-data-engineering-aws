Processed Spotify Data – Staging Layer

This folder represents the staging layer of the Spotify data pipeline.

The following CSV files are used in this project:

albums.csv artists.csv tracks.csv additional processed CSV files Due to file size limitations, the complete datasets are not uploaded directly to GitHub.

You can access the full processed data using the Google Drive link below: https://drive.google.com/drive/folders/1PgZQDvw5GnvVQuhV7-MtxIZHnLsZA-Zs

Alternatively, the data is stored in Amazon S3: s3://project-spotify-datewithdata-ay/staging/

These files are consumed by AWS Glue Visual ETL jobs for transformation and are later written to the data warehouse layer.
