# Téma projektu ETL datasetu
# 1. Úvod a popis zdrojových prác
Projekt sa zameriava na ETL procesy a analýzu dát v snowflake, warehouse platforma. s datasetom **MovieLens**. Využívame pritom spracované dáta do star schemy a následne ich zanalyzujeme dáta ako sú rozdelenie počtov používateľov na pohlavia, najlepšie hodnotené žánre či počet hodnotení na hodinu. 
## 1.1 Úvod a popis zdrojových dát
Cieľom semestrálnej práce je analýza dát o používateľoch, filmoch a hodnoteniach. Vďaka týmto údajom je možné odsledovať dôležité parametre a využiť to v prospech platformy ktoré sa následne dajú cieliť napríklad na vekovú danú skupinu a iné.
## 1.2 Popis tabuliek v ERD diagrame
   1. **Ratings** - Tabuľka obsahuje rating a má dátovy typ INT, čiže dáta sa nám budú ukladať v hodnotách od 1-5, rated_at ako DATETIME čiže budeme poznať dátum aj čas a následne je to spojovacia tabuľka medzi user_id a movie_id. Z tejto tabuľky je možné zistiť, ktorý uživateľ hodnotil aký film s akým hodnotením
   2. **Users** - Obsahuje údaje o pohlaví, PSČ, vek a ich pracovnej pozícii. Vek aj prac. pozícia sú foreign keys z tabuliek occupations a age_group. Tabuľka môže byť užitočná pri analýze rozdelenia počtu uživateľov na pohlavie.
   3. **Age_group** - Tabuľka obsahuje vekové skupiny, ktoré môžu slúžiť pre analýzu, ktorá nám zistí na ktoré vekové skupiny sa je dobré zamerať.
   4. **Occupations** - Tabuľka obsahuje názvy pracovných pozícii uživateľov
   5. **Tags** - Obsahuje samotné názvy tagov, kedy boli vytvorené, kto ich vytvoril a k akému filmu boli priradené. Analýza v pri danej tabuľke môže zahrňovať najhodnotenejšie tagy alebo presnejšie uviesť, o aký typ filmu sa môže jednať.
   6. **Movies** - Obsahuje meno filmu a v akom roku bol film vydaný.
   7. **Genres_movies** - Spojovacia tabuľka medzi tabuľkou genres a movies, keďže každý film môže mať viac žánrov a každý žáner je priradený k viacerým filmom. Analýzou tabuľky je možné zistiť, aké žánre filmov sú najpopulárnejšie.
   8. **Genres** - Obsahuje názvy žánrov
## 1.3 ERD diagram - Vzťahy medzi tabuľkami
Diagram nám znázorňuje, ako sú tabuľky prepojené a podľa toho vieme prispôsobiť naše analýzy.
<p align="center"> 
  <img src="https://github.com/SamoM225/db_semestralny_projekt/blob/main/erd_schema.PNG?raw=true" alt="Star Schema"> 
  <br> 
  <em>Obrázok 1 <br>Entitno-relačná schéma pre MovieLensDB</em> 
</p>

## 2 Dimenzionálny model
Pre návrh dimenzionálnych tabuliek sme si vytvorili faktovú tabuľku fact_rating a následne 5 dimenzionálnych tabuliek.
### 2.1 Dimenzionálne tabuľky
 - **Dim_movies** - Obsahuje názov filmu, žáner a rok vydania.
 - **Dim_users** - Obsahuje vekovú skupinu, pohlavie, pracovnú pozíciu a PSČ.
 - **Dim_tags** - Obsahuje movie_id, názov tagu a created_at.
 - **Dim_date** - Obsahuje deň, mesiac, rok a dátum ako celok v stlpci.
 - **Dim_time** - Obsahuje hodinu, minutu, sekundu a čas ako celok v stlpci.
### 2.2 Faktová tabuľka
Výber faktovej tabuľky je najdôležitejší, preto sme zvolili ratings ako faktovú tabuľku. Každý fakt ma zaznamenaný datum a čas, kedy bol pridaný, tag a následne cudzie klúčec z dimenzionýlnych tabuliek.
<p align="center"> 
  <img src="https://github.com/SamoM225/db_semestralny_projekt/blob/main/star_schema.PNG?raw=true" alt="Star Schema"> 
  <br> 
  <em>Obrázok 2 <br>Schéma hviezdy pre MovieLensDB</em> 
</p>

## 3. ETL Procesy v Snowflake
Pre vykonanie ETL procesu si musíme vytvoriť stage a stage tabuľky, ktoré budú slúžiť ako dočasné úložisko dát, ktoré budú na konci .sql súboru vymazané. Po vytvorení stage-u musíme vytvoriť dané tabuľky, do ktorých budeme následne vkladať - **extrahovať** naše údaje, ktoré potom budeme môcť **transformovať** do dimenzionálnych tabuliek a na záver bude **load** proces.
### 3.1 Extract
Táto časť dokumentu je zameraná na extrahovanie dát z datasetu s príponou .csv do už spomenutých stage tabuľiek. Predtým, než budeme pokračovať, je nutné si najskôr vytvoriť stage tabulky podľa ERD schemy a následne vytvoriť stage.
- **Query pre vytvorenie stage**:
```SQL
CREATE OR REPLACE STAGE movielens_stage;
```
Teraz môžme načítať do stage-u pomocou snowflake dáta a kopírovať ich do staging tabuliek
- **Query pre kopírovanie dát**:
```SQL
COPY INTO users_staging
FROM @movielens_stage/users.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```
Do staging tabulky users_staging kopírujeme dáta zo stagingu `movielens` a spravíme tak pre každú tabuľku

### 3.2 Transform
Transform fáza je zameraná na spracovanie a úpravu dát zo staging tabuliek do dimenzionálnych tabuliek a do faktovej tabuľky. 
#### 3.2.1 Vytvorenie dimenzionálnych tabuliek a faktovej tabuľky.
Ak boli všetky naše kroky správne, následná analýza dát je jednoduchšia a efektívnejšia. Každá z našich dimenzionálnych tabuliek obsahuje potrebné údaje pre analýzu a zároveň sú to referenčné tabuľky pre faktovú tabuľku.

- **dim_movies**
```SQL
CREATE OR REPLACE TABLE dim_movies AS
SELECT DISTINCT 
	m.id AS dim_movieid,
	m.title AS title,
	g.name AS genre,
	m.release_year AS release_year,
FROM movies_staging m
JOIN genres_movies_staging gr ON m.id = gr.movie_id
JOIN genres_staging g ON gr.genre_id = g.id;
```
- **dim_users**
```SQL
CREATE OR REPLACE TABLE dim_users AS
SELECT DISTINCT
	u.id AS dim_userid,
	ag.name AS age_group,
	u.gender,
	o.name AS occupation,
	u.zip_code AS zip_code,
FROM users_staging u
JOIN age_group_staging ag ON u.age = ag.id
JOIN occupations_staging o ON u.occupation_id = o.id;
```
- **dim_date**
```SQL
CREATE OR REPLACE TABLE dim_date AS 
SELECT
	ROW_NUMBER() OVER (ORDER BY CAST(rated_at AS DATE)) AS dim_dateid, 
	DATE_PART(day, rated_at) AS day,
	DATE_PART(month, rated_at) AS month,
	DATE_PART(year, rated_at) AS year,
	CAST(rated_at AS DATE) AS date
FROM ratings_staging
GROUP BY CAST(rated_at AS DATE),
	DATE_PART(day, rated_at),
	DATE_PART(month, rated_at),
	DATE_PART(year, rated_at);
```
- **dim_time**
```SQL
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
	ROW_NUMBER() OVER (ORDER BY DATE_PART(hour, rated_at), DATE_PART(minute, rated_at), 
	DATE_PART(second, rated_at)) AS dim_timeid,
	(hour, rated_at) AS hour,
	(minute, rated_at) AS minute,
	(second, rated_at) AS second
FROM ratings_staging
BY 
	DATE_PART(hour, rated_at),
	DATE_PART(minute, rated_at),
	DATE_PART(second, rated_at)
BY 
	DATE_PART(hour, rated_at),
	DATE_PART(minute, rated_at),
	DATE_PART(second, rated_at);
```
- **dim_tags**
```SQL
OR REPLACE TABLE dim_tags AS
DISTINCT
	.id,
	.movie_id,
	.user_id,
	.tags AS tag,
	.created_at
FROM tags_staging tg;
```
### 3.2 Load
Po úspešnom vytvorení dimenzií a faktovej tabuľky máme dáta načítané do konečného formátu. Na koniec môžeme vymazať staging tabulky aby sme efektívnejšie využili úložisko.
- **Query pre vymazanie staging tabuliek:**
```SQL
DROP TABLE IF EXISTS age_group_staging; 
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
DROP TABLE IF EXISTS users_staging;
```
## 4. Vizualizácia dát
<p align="center"> 
  <img src="https://github.com/SamoM225/db_semestralny_projekt/blob/main/grafy_dashboard.PNG?raw=true" alt="Star Schema"> 
  <br> 
  <em>Obrázok 3 <br>Dashboard s grafmi pre MovieLens</em> 
</p>

**Graf 1: Rozdelenie uživatelov podľa pohlavia**
Graf nám zobrazuje rozdelenie užívateľov na pohlavie.
```SQL
SELECT 
	u.gender AS user_gender,
	COUNT(r.fact_ratingid) AS num_unique_ratings
FROM 
	fact_rating r
JOIN 
	dim_users u ON r.dim_userid = r.fact_ratingid
GROUP BY 
	u.gender
ORDER BY 
	num_unique_ratings DESC;
```
**Graf 2: Počet hodnotení na vekovú skupinu**
Graf nám zobrazuje Hodnotenia, ktoré vytvorila každá veková skupina. Graf nám pomáha zistiť, ktorá veková skupina má najčastejšiu interakciu s filmami.
```SQL
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
```
**Graf 3: Priemerné hodnotenie na žáner**
Graf nám zobrazuje priemerné hodnotenie filmu na žáner. Graf nám pomáha zistiť, ktoré žánre sú najviac trendy.
```SQL
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
```
**Graf 4: Počet hodnotení na hodinu (0-24)**
Graf nám ukazuje, kedy uživatelia najčastejšie interagujú (hodnotia) filmy počas danej hodiny.
```SQL
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
```
-**Graf 5: Počet filmov na žáner**
Graf nám zobrazuje počet vydaných filmov na žáner. Pri zakomponovaní viacerých údajov je možné zistiť, ako sa menila krivka a ako sa menili najčastejšie žánre filmov.
```SQL
SELECT 
	m.genre AS genre,
	COUNT(m.dim_movieid) AS movie_count
FROM dim_movies m
GROUP BY m.genre
ORDER BY movie_count DESC
LIMIT 10;
```
#
Autor: Samuel Majerčík
