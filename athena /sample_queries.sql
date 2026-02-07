-- Preview transformed dataset
SELECT *
FROM spotify.datawarehouse
LIMIT 10;

-- Fetch artist names with track IDs
SELECT name, track_id
FROM spotify.datawarehouse
LIMIT 10;

-- Top artists by followers
SELECT name, followers
FROM spotify.datawarehouse
ORDER BY followers DESC
LIMIT 10;
