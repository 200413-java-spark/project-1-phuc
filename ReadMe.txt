# Project 1 - Phuc Le
## Servlet Database ETL Processor

## Command to run on vs code
> java -Xmx1g -jar target/chessdb-0.0.1-SNAPSHOT.jar

## Commands to start up Docker container and Postgres SQL server
> sudo docker run --name chessdb -d --rm -p 5432:5432 chessdbtest
> sudo docker exec -it chessdb psql -U chessdb

## Contains two servlets
### tag: chessdb/spark
> curl GET localhost:8080/chessdb/spark
	-needs a file parameter: example. ?file=games.csv
> curl -X Post localhost:8080/chessdb/spark

Parameters
```
file
```
-[filename]
	-file should be in the resource folder
	
```
op
```
-count
	-counts the total number of values in selected column
	-available for single column mode
-ave
	-takes the mean of values in selected column
	-available for single and double column mode
-top
	-sorts by key for the top 20 of a column
	-available for double column mode only
-topave
	-sorts for the top 20 of column 1 based on column 2
	-available for double column mode only
```
save
```
-yes
	-if "yes" saves the processed data into sql database
```
col1
```
-[integer]
	-selects the first column
	-is the key in double column mode
```
col2
```
-[integer]
	-selects the second column

### tag: chessdb/sql
> curl -X GET localhost:8080/chessdb/sql
```
table
```
-[name of table]
	-pulls table from the sql database
> curl -X POST localhost:8080/chessdb/sql
```
table
```
-[name of table]
	-deletes the table
