-- Graf 1 - Rozdelenie uživateľov podľa pohlavia
SELECT
    u.gender AS user_gender,
    COUNT(DISTINCT r.fact_ratingid) AS num_unique_ratings
FROM fact_rating r
    JOIN dim_users u ON r.dim_userid = u.dim_userid
GROUP BY
    u.gender
ORDER BY num_unique_ratings DESC;
-- Graf 2 - Rozdelenie uživateľov podľa veku
SELECT 
    u.age_group AS age_group,
    COUNT(fr.fact_ratingid) AS num_ratings
FROM 
    fact_rating fr
JOIN 
    dim_users u ON fr.dim_userid = u.dim_userid
GROUP BY 
    u.age_group
ORDER BY 
    num_ratings DESC;
-- Graf 3 - Priemerne hodnotenie na žáner
SELECT 
    m.genre AS genre, 
    ROUND(AVG(r.rating), 1) AS avg_rating,
    COUNT(r.rating) AS ratings
FROM 
    fact_rating r
JOIN 
    dim_movies m ON r.dim_movieid = m.dim_movieid
GROUP BY 
    m.genre
ORDER BY 
    avg_rating DESC
LIMIT 10;
-- Graf 4 - Priemerne hodnotenie na hodinu (0-23)
SELECT 
    DATE_PART(hour, fr.rated_at) AS rating_hour, 
    COUNT(*) AS rating_count,
    ROUND(AVG(fr.rating),1) AS avg_rating,
FROM 
    fact_rating fr
GROUP BY 
    DATE_PART(hour, fr.rated_at)
ORDER BY 
    rating_hour;
-- Graf 5 - Počet filmov na žáner
SELECT 
    m.genre AS genre,
    COUNT(m.dim_movieid) AS movie_count
FROM dim_movies m
GROUP BY m.genre
ORDER BY movie_count DESC
LIMIT 10;
