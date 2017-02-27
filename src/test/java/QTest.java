/**
 * Created by allen on 2017/1/9.
 */
public class QTest {

    public static void main(String argus[]){
    }
}

/*
项目打包, 并发送jar包到etl服务器
scp target/prestoudf-jar-with-dependencies.jar isuhadoop@192.168.220.145:/tmp
/usr/lib/presto/bin/presto-cli --catalog hive --server 192.168.220.136:8285

查询日志结果
ansible emr,dn -m shell -a  'cat /var/log/presto/server.log | grep "output long called"' --private-key ~/etl.pem | more
ansible emr,dn -m shell -a  'tail -f /var/log/presto/server.log | grep "output long called"' --private-key ~/etl.pem | more
ansible emr,dn -m shell -a  'tail -100 /var/log/presto/server.log' --private-key ~/etl.pem

set session processing_optimization='columnar';
set session optimize_metadata_queries=true;
set session task_concurrency=32;
*/

/*
CREATE TABLE `events2_orc_b2`(
  `xwho` BIGINT,
  `xwhen` string,
  `xwhere` string,
  `xwhat` string,
  `xcontext` map<string,string>)
PARTITIONED BY (
  `appid` string,
  `ds` string)
stored as orc
TBLPROPERTIES
('orc.create.index'='true',
"orc.compress"="snappy",
"orc.stripe.size"="268435456",
"orc.row.index.stride"="10000")


insert into tablename select * from tablename where ds = '2016-12-01' order by xwhen;
 */
