# data_disposal

Do you have data in Apache Hadoop using Apache HDFS that is made available with Apache Hive? Do you spend
too much time manually cleaning old data or maintaining multiple hacked
scripts? If you answered yes to these questions, then the Data Disposal
tool is for you!

The Java based Data Disposal tool takes in a simple yaml configuration specifying Apache HDFS directories
and Apache Hive tables with customizable retention windows and date parsing from
a partition or file path.

## Apache HDFS Configurations
* int **retentionDuration**: How many units of **granularity** the retention should be.
* ChronoUnit **granularity**: A string that can be parsed into a ChronoUnit value
for example `DAYS` or `WEEKS`.
* SimpleDateFormat **dateFormat**: A string that that matches the allowed patterns in the
[DateTimeFormatter spec](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
* String **path**: A path in Apache HDFS. If specifying **dateFormat**, you need to specify `%s` where you expect
the date string to be located. Note that it will match the regex even where a more precise one could be used
(e.g. 'yyyyMMdd' will also match a path containing '20160229125500'). All paths will be treated as globs.
* HDFSRetentionType **retentionType**: Either `MODIFICATION_TIME` for retention
based on the specified file/folders modification time or `PATH_DATE` for retention
based on the **dateFormat** included in the path.
* boolean **recursive**: If the specified path/s up for disposal are directories,
should they be deleted?

## Apache Hive Configurations
* int **retentionDuration**: How many units of **granularity** the retention should be.
* ChronoUnit **granularity**: A string that can be parsed into a ChronoUnit value
for example `DAYS` or `WEEKS`.
* SimpleDateFormat **dateFormat**: A string that that matches the allowed patterns in the
[DateTimeFormatter spec](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
* String **database**: The database containing the table.
* String **table**: The table that needs some partitions removed.
* String **partitionFilterKey**: Which key contains the date stamp that matches **dateFormat**:
* boolean **deleteExternalData**: Delete data associated with the partions that are going to
be deleted. **NOTE**: This only works for Apache Hive internal tables. For external tables, you need to
delete the data files in addition to the partitions.

## Example Config
### Apache Hive Example
[HiveConfigExample.yaml](./src/resources/HiveConfigExample.yaml)
```
entries:
  - database: somename
    table: users
    partitionFilterKey: date
    deleteExternalData: true
    retentionDuration: 14
    granularity: DAYS
    dateFormat: yyyy-MM

  - database: somename2
    table: comments
    partitionFilterKey: date
    deleteExternalData: false
    retentionDuration: 14
    granularity: WEEKS
    dateFormat: yyyy-MM
```

### Apache HDFS Example
[HDFSConfigExample.yaml](./src/resources/HDFSConfigExample.yaml)
```
hdfsNamenode: "hdfs://namenode:8020"

entries:
  - path: hdfs://namenode:8020/somepath/data/date=%s
    retentionDuration: 14
    granularity: DAYS
    retentionType: PATH_DATE
    dateFormat: yyyy-MM-dd
    recursive: true

  - path: hdfs://namenode:8020/somepath/metadata/somepath
    retentionDuration: 14
    granularity: HOURS
    retentionType: MODIFICATION_TIME
    recursive: false
```

## Install
Currently the jar is not distributed to any repositories. You can create a working
jar by cloning the repo and running `mvn clean package`

## Usage
You must specify the Apache Hive conf dir in your classpath and you also need to
ensure that both Apache Hadoop and Hive jars are on the classpath. In the example here,
`hive --service jar` is used to include all Hadoop and Hive dependencies.

The `--dry_run` option will allow you to run the script to just log all directories
and partitions that would be deleted in an actual run of the tool.

`HADOOP_CLASSPATH=/<your path to>/hive/conf hive --service jar /<your path to>/data_disposal.jar com.vz.disposal.DataDisposal --hive_conf /<your path to>/hive_config.yaml --hdfs_conf /<your path to>hdfs_config.yaml --dry_run`

## Retention for other datastores
The data disposal tool is designed with a config interface and a data deletion interface both of which could easily be extended
to new datastores based on your requirements. We would be happy to accept contributions following the guidelines listed below.

## Contribute
* See [Contributing](Contributing.md)
* See [Code-of-Conduct](Code-of-Conduct.md)

## License
* See [LICENSE](LICENSE)
