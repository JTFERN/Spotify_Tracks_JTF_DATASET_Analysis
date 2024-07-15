create database Pipedrive;
drop table if exists `musica`;
CREATE TABLE `musica` (
  `id` int DEFAULT NULL,
  `track_id` text,
  `artists` text,
  `album_name` text,
  `track_name` text,
  `popularity` int DEFAULT NULL,
  `duration_ms` int DEFAULT NULL,
  `explicit` text,
  `danceability` double DEFAULT NULL,
  `energy` double DEFAULT NULL,
  `key` int DEFAULT NULL,
  `loudness` double DEFAULT NULL,
  `mode` int DEFAULT NULL,
  `speechiness` double DEFAULT NULL,
  `acousticness` double DEFAULT NULL,
  `instrumentalness` double DEFAULT NULL,
  `liveness` double DEFAULT NULL,
  `valence` double DEFAULT NULL,
  `tempo` double DEFAULT NULL,
  `time_signature` int DEFAULT NULL,
  `track_genre` text);
  
LOAD DATA INFILE 'C:/Users/jotfe/OneDrive/Desktop/PYTHON/PIPEDRIVE/dataset.csv' 
INTO TABLE `musica`
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from musica where artists="";
delete from musica where artists="";

select id,
case
when popularity>=90 then 10
when popularity>=80 then 9
when popularity>=70 then 8
when popularity>=60 then 7
when popularity>=50 then 6
when popularity>=40 then 5
when popularity>=30 then 4
when popularity>=20 then 3
when popularity>=10 then 2
when popularity<10 then 1
else 0
end as popkk
from musica
order by id;

drop view if exists pop;
create view pop as
select *,
case
when popularity>=90 then 10
when popularity>=80 then 9
when popularity>=70 then 8
when popularity>=60 then 7
when popularity>=50 then 6
when popularity>=40 then 5
when popularity>=30 then 4
when popularity>=20 then 3
when popularity>=10 then 2
when popularity<10 then 1
else 0
end as poprank,
duration_ms/60000 as minutes
from musica
order by id;

#Vamos começar por ver quantas entradas temos
select count(*) as 'Number of entries'  from pop;
#Quantas musicas distintas
select count(distinct(track_id)) from pop;
#Quantos géneros temos
select count(distinct(track_genre)) from pop;
#Musicas por género
select track_genre,count(track_id) from pop group by track_genre;
#Muito género, vamos ver top 20 e bottom 20
select track_genre as 'Track genre', avg(popularity) as AVGpopularity from pop group by track_genre order by AVGpopularity desc limit 20;
select track_genre as 'Track genre', avg(popularity) as AVGpopularity from pop group by track_genre order by AVGpopularity limit 20;
#Distribuicao da pop
select poprank as PopularityRank, count(distinct(track_id)) Count, count(distinct(track_id))/(select count(distinct(track_id)) from pop)*100 as 'ID%' from pop group by poprank order by poprank desc;

#non numeric - explicit, key, mode, time
#explicit
select explicit, count(distinct(track_id)) as 'Tracks', count(distinct(track_id))/(select count(distinct(track_id)) from pop)*100 as 'ID%', avg(popularity) AVGpopularity from pop group by explicit order by explicit desc;
select poprank,explicit, avg(popularity) from pop group by poprank,explicit order by poprank desc;
with averagepopg as (select track_genre, avg(popularity) as avgpop from pop group by track_genre)
select pop.track_genre as 'Most explicit genres', count(id) Count, avgpop AVGpopularity from pop join averagepopg on pop.track_genre=averagepopg.track_genre where explicit='True' group by pop.track_genre order by count(id) desc limit 20;

#key e mode
select pop.key, count(id),count(id)/(select count(id) from pop)*100 as 'ID%' from pop group by  pop.key order by count(id) desc;
select pop.key, avg(popularity) AVGpopularity from pop group by pop.key order by AVGpopularity desc;
select pop.mode, count(id),count(id)/(select count(id) from pop)*100 as 'ID%' from pop group by  pop.mode order by count(id) desc;
select pop.mode, avg(popularity) AVGpopularity from pop group by pop.mode order by AVGpopularity desc;


#time
select time_signature,count(id)/(select count(id) from pop)*100 as 'ID%' from pop group by  time_signature order by count(id) desc;
with averagepopg as (select track_genre, avg(popularity) as avgpop from pop group by track_genre)
select pop.track_genre,count(id) as Tracks,avgpop as AVGpopularity from pop join averagepopg on pop.track_genre=averagepopg.track_genre where time_signature!=4 group by track_genre order by count(id) desc limit 20;


#Variaveis numericas
select poprank, count(distinct(track_id)) as Tracks, count(distinct(track_id))/(select count(distinct(track_id)) from pop)*100 as 'id%', round(avg(duration_ms),2) as 'duration(ms)', round(avg(minutes),2) as minutes,round(avg(danceability),2) as danceability,round(avg(energy),2) as energy,round(avg(loudness),2) as loudness,round(avg(speechiness),2) as speechiness,round(avg(acousticness),2) as acousticness, round(avg(instrumentalness),3) as instrumentalness,round(avg(liveness),2) as liveness,round(avg(valence),2) as valence,round(avg(tempo),2) as tempo from pop group by poprank order by poprank desc;

select track_genre as 'Track genre', avg(popularity) as AVGpopularity,round(avg(duration_ms),2) as 'duration(ms)', round(avg(minutes),2) as minutes,round(avg(danceability),2) as danceability,round(avg(energy),2) as energy,round(avg(loudness),2) as loudness,round(avg(speechiness),2) as speechiness,round(avg(acousticness),2) as acousticness, round(avg(instrumentalness),3) as instrumentalness,round(avg(liveness),2) as liveness,round(avg(valence),2) as valence,round(avg(tempo),2) as tempo from pop group by track_genre order by AVGpopularity desc limit 20;
select track_genre as 'Track genre', avg(popularity) as AVGpopularity,round(avg(duration_ms),2) as 'duration(ms)', round(avg(minutes),2) as minutes,round(avg(danceability),2) as danceability,round(avg(energy),2) as energy,round(avg(loudness),2) as loudness,round(avg(speechiness),2) as speechiness,round(avg(acousticness),2) as acousticness, round(avg(instrumentalness),3) as instrumentalness,round(avg(liveness),2) as liveness,round(avg(valence),2) as valence,round(avg(tempo),2) as tempo from pop group by track_genre order by AVGpopularity limit 20;

select track_genre as 'Track genre', avg(popularity) as AVGpopularity,round(avg(duration_ms),2) as 'duration(ms)', round(avg(minutes),2) as minutes,round(avg(danceability),2) as danceability,round(avg(energy),2) as energy,round(avg(loudness),2) as loudness,round(avg(speechiness),2) as speechiness,round(avg(acousticness),2) as acousticness, round(avg(instrumentalness),3) as instrumentalness,round(avg(liveness),2) as liveness,round(avg(valence),2) as valence,round(avg(tempo),2) as tempo from pop where track_genre in ('emo','latin')group by track_genre order by AVGpopularity desc limit 20;

#Speechiness %
select count(distinct(track_id))/(select count(distinct(track_id)) from pop)*100 from pop where speechiness<0.34