# Summary Discription:
This project try to build schema an ETL pipeline for a database hosted on AWS S3 bucket.\
building an ETL pipeline that extracts their data from S3, processes them using Spark,
and\
loads the data back into S3 as a set of dimensional tables.


# Requirments
1- creat a s3 bucket, datalake-result, with assign the path to output_data variable to save results in\
2- creat EMR cluster in able to use EMR notebook\
3- create an EMR notebook to write code in


# How to Run This Project:
Use the EMR notebook created to run the code.



# Data and Schema:
## *Data*

**1- Song datasets**: all json files are nested in subdirectories under /data/song_data.

ex:

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7",
"artist_latitude": null, "artist_longitude": null, "artist_location": "",
"artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title":
"Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
`

**2- Log datasets**: all json files are nested in subdirectories under /data/log_data. 

ex:

`
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden",
"gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,
"level":"paid","location":"New York-Newark-Jersey City, NY-NJPA",
"method":"PUT","page":"NextSong","registration":1540283578796.0,
"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,
"ts":1541639510796,
"userAgent":"\"Mozilla\/5.0(WindowsNT6.1)AppleWebKit\/537.36(KHTML,likeGecko)Chrome\/36.0.1985.143Safari\/537.36\"","userId":"20"}
`

## *Schema*

The schema used for this exercise is the Star Schema: There is one main fact table containing all the measures 
associated to each event (user song plays), and 4 dimentional tables

#### Fact Table

**songplays** - records in log data associated with song plays i.e. records with page NextSong
cols: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
**users** - users in the app
, cols : user_id, first_name, last_name, gender, level

**songs** - songs in music database
, cols : song_id, title, artist_id, year, duration

**artists** - artists in music database
, cols : artist_id, name, location, latitude, longitude

**time** - timestamps of records in songplays broken down into specific units
, cols : start_time, hour, day, week, month, year, weekday



![s3 bucket after saving files](https://github.com/Anas-Rabea/ITI/blob/main/Spark/s3%20savings.jpeg)


