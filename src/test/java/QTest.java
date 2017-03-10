/**
 * Created by allen on 2017/1/9.
 */
public class QTest {

    public static void main(String argus[]){

    }
}

/*
项目打包, 并发送jar包到etl服务器
~/Maven/bin/mvn clean compile assembly:assembly

测试服务器1
scp target/prestoudf-jar-with-dependencies.jar isuhadoop@192.168.220.145:/tmp
/usr/lib/presto/bin/presto-cli --catalog hive --server 192.168.220.136:8285

测试服务器2
scp target/prestoudf-jar-with-dependencies.jar isuhadoop@192.168.0.36:/tmp
/usr/lib/presto/bin/presto-cli --catalog hive --server 192.168.0.31:8285

优化参数
set session processing_optimization='columnar';
set session optimize_metadata_queries=true;
set session task_concurrency=32;
 */

/*
CREATE TABLE `tablename`(
  `xwho` BIGINT,
  `xwhen` string,
  `xwhere` string,
  `xcontext` map<string,string>)
PARTITIONED BY (
  `appid` string,
  `ds` string,
  `xwhat` string)
stored as Parquet
TBLPROPERTIES
('orc.create.index'='true',
"orc.compress"="SNAPPY",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="10000")

insert into tablename select * from tablename where ... order by xwhen;
 */

/*
#!/bin/sh

workDir=`dirname $0`
workDir=`cd ${workDir};pwd`

work_jar=/tmp/prestoudf-jar-with-dependencies.jar
dest=/usr/lib/presto/lib/plugin/eg/

for node in `cat ${workDir}/nodes`
do
    echo "=============${node}================="
    ssh ${node} "sudo rm -rf ${dest}/prestoudf-jar-with-dependencies.jar"

    scp -r ${work_jar} root@$node:${dest}
    ssh ${node} "sudo ls -l ${dest}"

    ssh ${node} "sudo service presto stop"
    ssh ${node} "sudo service presto start"
    ssh ${node} "sudo service presto status"
done
 */
