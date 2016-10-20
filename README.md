# TD2 Hadoop

### General Information

+ I used IntelliJ to create this project
+ I used Maven to compile it
+ Then once my .jar file created, I pushed it with the 'scp' command onto the remote server gdamien@ns3024382
+ Finally I used the 'yarn jar' command to execute the jar on the prenoms.csv file located on HDFS

### Additionnal Question

+ For each MR, can we use the combiner ? Why ?

For the first two MR applications, it is ok to use a combiner since the sum is commutative and associative. It creates no issues between the map and reduce phases. However, when it comes to the third application (average of M and F), using a combiner could result in a different result since the mean is not commutative and associative. For this task, I decided not to use the combiner.

### Hive Part

In order to have my tables to work on, I first created a DATABASE called test using the following command in Hive:

CREATE DATABASE test LOCATION ‘/user/gdamien/test’;

Then I created a folder containing the prelims.csv file so that I could use it afterward once my tables created:

```
hdfs dfs -mkdir prenoms
hdfs dfs -cp /res/prenoms.csv prenoms/0000
```

Now I could create my two tables in Hive:

```
CREATE TABLE prenoms3(
    prenoms STRING,
    gender array<string>,
    origin array<string>,
    version DOUBLE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\073'
    collection items terminated by ',' 
    STORED AS TEXTFILE LOCATION '/user/gdamien/prenoms';

CREATE TABLE prenoms_opt3(
    prenoms STRING,
    gender array<string>,
    origin array<string>,
    version DOUBLE)
    ROW FORMAT DELIMITED
    STORED AS ORC;
```
Finally I loaded the data in prenoms3 and prenoms_opt3 using the following commands in Hive:
```
LOAD DATA INPATH '/user/gdamien/prenoms' INTO TABLE prenoms3;

INSERT INTO TABLE prenoms_opt3 SELECT * FROM prenoms3;
```
Then I could play with Hive ! (see the query file)
