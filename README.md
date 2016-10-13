# TD2 Hadoop

### General Information

+ I used IntelliJ to create this project
+ I used Maven to compile it
+ Then once my .jar file created, I pushed it with the 'scp' command onto the remote server gdamien@ns3024382
+ Finally I used the 'yarn jar' command to execute the jar on the prenoms.csv file located on HDFS

### Additionnal Question

+ For each MR, can we use the combiner ? Why ?

For the first two MR applications, it is ok to use a combiner since the sum is commutative and associative. It creates no issues between the map and reduce phases. However, when it comes to the third application (average of M and F), using a combiner could result in a different result since the mean is not commutative and associative. For this task, I decided not to use the combiner.
