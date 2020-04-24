## Data Modeling for Sparkify.
***

### Problem
Sparkify needs to do analysis on the song and user activity on their music streaming app for unserstanding what songs users are listening to.
### Purpose
Given songs and user activity data that resided in S3, create a database on Redshift and create and ELT pipeline that extracts the data and stage on the database and uses to create dimensional  tables optimized for song play **analysis** .


### Goals Achieved
For the Pupose of this project a databse waas created on redshift, 2 staging tables were created: *staging_events, staging_songs* , and trasformed into 4 dimension tables: *users, songs, artists* and *time* linked to 1 facts table *songplays* to form a star schema that will allow for easy analysis.

Analytical queries can now be performed on the facts table songplays to answer questions like "How many".

ELT Pipeline were also created for running queries on the data provided from **log** and **song**  data sets for songs and user activity.

sortkey



### Sample Queries
A sample query that can be performed for song play anylysis.

1. How many times an artist song has been played and order by artist name.

##### Query
`SELECT a.name, count(*) AS numberOfPlays FROM artists  a JOIN songplays s ON a.artist_id=s.artist_id GROUP BY a.Name order by a.name asc`



2. How many songs have been played for every location and order by location name

##### Query
`select location, count(*) as numberOfPlays from songplays group by location order by location asc`
