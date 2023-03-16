SELECT song_name, artist_name, played_at, played_date
	FROM public.spotify_tracks_master;

--Sure, here are some SQL queries that can help analyze the data:

--Count the number of unique songs and artists in the table:

SELECT COUNT(DISTINCT song_name) AS unique_songs, COUNT(DISTINCT artist_name) AS unique_artists
FROM public.spotify_tracks_master;

--Find the top 10 most played songs:

SELECT song_name, COUNT(*) AS plays
FROM public.spotify_tracks_master
GROUP BY song_name
ORDER BY plays DESC
LIMIT 10;

--Find the top 10 most played artists:
SELECT artist_name, COUNT(*) AS plays
FROM public.spotify_tracks_master
GROUP BY artist_name
ORDER BY plays DESC
LIMIT 10;

--Find the average number of plays per day:
SELECT AVG(play_count) AS avg_plays_per_day
FROM (
  SELECT played_date, COUNT(*) AS play_count
  FROM public.spotify_tracks_master
  GROUP BY played_date
) subquery;

--Find the date with the highest number of plays:
SELECT played_date, COUNT(*) AS plays
FROM public.spotify_tracks_master
GROUP BY played_date
ORDER BY plays DESC
LIMIT 1;
-- Find the average time between plays:
SELECT AVG(time_between_plays) AS avg_time_between_plays
FROM (
  SELECT played_at - LAG(played_at) OVER (ORDER BY played_at) AS time_between_plays
  FROM public.spotify_tracks_master
) subquery;

SELECT 
  song_name, 
  artist_name, 
  played_at, 
  played_date, 
  (TO_TIMESTAMP(played_at, 'YYYY-MM-DD HH24:MI:SS') - 
   LAG(TO_TIMESTAMP(played_at, 'YYYY-MM-DD HH24:MI:SS')) OVER (ORDER BY TO_TIMESTAMP(played_at, 'YYYY-MM-DD HH24:MI:SS'))) AS time_diff
FROM public.spotify_tracks_master;
