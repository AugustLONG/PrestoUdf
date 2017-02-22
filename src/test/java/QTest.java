/**
 * Created by allen on 2017/1/9.
 */
public class QTest {

    public static void main(String argus[]){

        long start = System.currentTimeMillis();
        for (int j = 0; j < 100000000; ++j) {
            int a = (j - 50) / 186400;
        }
        System.out.println(System.currentTimeMillis() - start);
    }
}

/*
项目打包, 并发送jar包到etl服务器
mvn clean compile assembly:assembly
scp -i ~/etl_emr.pem target/prestoudf-jar-with-dependencies.jar ec2-user@ec2-54-223-136-157.cn-north-1.compute.amazonaws.com.cn:/tmp

ansible emr,dn -m shell -a  'sudo rm -rf /usr/lib/presto/plugin/reyun/*' --private-key ~/etl.pem
ansible emr,dn -m shell -a  'sudo ls /usr/lib/presto/plugin/reyun/' --private-key ~/etl.pem
ansible emr,dn -m copy -a "src=/tmp/prestoudf-jar-with-dependencies.jar dest=/usr/lib/presto/plugin/reyun" --sudo --private-key ~/etl.pem
ansible emr,dn -m shell -a  'sudo ls /usr/lib/presto/plugin/reyun/' --private-key ~/etl.pem

ansible emr,dn -m shell -a  'sudo stop presto-server' --private-key ~/etl.pem
ansible emr,dn -m shell -a  'sudo start presto-server' --private-key ~/etl.pem
ansible emr,dn -m shell -a  'sudo status presto-server' --private-key ~/etl.pem

查询日志结果
ansible emr,dn -m shell -a  'cat /var/log/presto/server.log | grep "output long called"' --private-key ~/etl.pem | more
ansible emr,dn -m shell -a  'tail -f /var/log/presto/server.log | grep "output long called"' --private-key ~/etl.pem | more
ansible emr,dn -m shell -a  'tail -100 /var/log/presto/server.log' --private-key ~/etl.pem

http://ec2-54-223-214-49.cn-north-1.compute.amazonaws.com.cn:8889/

ssh -i ~/etl_emr.pem ec2-user@ec2-54-223-214-49.cn-north-1.compute.amazonaws.com.cn
ssh -i ~/etl_emr.pem ec2-user@ec2-54-223-136-157.cn-north-1.compute.amazonaws.com.cn

代理
ssh -i ~/etl_emr.pem -N -D 8157 hadoop@ec2-54-223-214-49.cn-north-1.compute.amazonaws.com.cn

set session processing_optimization='columnar';

set session optimize_metadata_queries=true;

set session task_concurrency=32;
*/
