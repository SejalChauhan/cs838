query=$1

echo "Query ($query)"
mkdir query$query

#clearing the caches on slave vms
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"

ssh vm2 << HERE
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
HERE

ssh vm3 << HERE 
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
HERE

ssh vm4 << HERE
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
HERE

#clearing hadoop fs history folder
hadoop fs -rm -r -f /tmp/hadoop-yarn/staging/history/*

#collecting the network and disk bandwidth stats before
sudo cat /proc/net/dev > query$query/netbeforevm1.txt
sudo cat /proc/diskstats > query$query/diskbeforevm1.txt

ssh vm2 << HERE
sudo cat /proc/net/dev > netbeforevm2.txt

sudo cat /proc/diskstats > diskbeforevm2.txt
scp netbeforevm2.txt vm1:~/logs-trial/query$query/
scp diskbeforevm2.txt vm1:~/logs-trial/query$query/
rm -rf netbeforevm2.txt diskbeforevm2.txt
HERE

ssh vm3 << HERE
sudo cat /proc/net/dev > netbeforevm3.txt
sudo cat /proc/diskstats > diskbeforevm3.txt
scp netbeforevm3.txt vm1:~/logs-trial/query$query/
scp diskbeforevm3.txt vm1:~/logs-trial/query$query/
rm -rf netbeforevm3.txt diskbeforevm3.txt
HERE

ssh vm4 << HERE
sudo cat /proc/net/dev > netbeforevm4.txt
sudo cat /proc/diskstats > diskbeforevm4.txt
scp netbeforevm4.txt vm1:~/logs-trial/query$query/
scp diskbeforevm4.txt vm1:~/logs-trial/query$query/
rm -rf netbeforevm4.txt diskbeforevm4.txt
HERE

#run the desired query
cd ../workload/hive-tpcds-tpch-workload
(hive --hiveconf hive.execution.engine=mr --hiveconf hive.cbo.enable=true -f sample-queries-tpcds/query$query.sql --database tpcds_text_db_1_50) 2> output/query_mr.out
cd -
mv ../workload/hive-tpcds-tpch-workload/output/query_mr.out query$query/

#copying logs jhist file locally
hadoop fs -get /tmp/hadoop-yarn/staging/history/ query$query/

#collecting the network and disk bandwidth stats before
sudo cat /proc/net/dev > query$query/netaftervm1.txt
sudo cat /proc/diskstats > query$query/diskaftervm1.txt

ssh vm2 << HERE
sudo cat /proc/net/dev > netaftervm2.txt
sudo cat /proc/diskstats > diskaftervm2.txt
scp netaftervm2.txt vm1:~/logs-trial/query$query/
scp diskaftervm2.txt vm1:~/logs-trial/query$query/
rm -rf netaftervm2.txt diskaftervm2.txt
HERE

ssh vm3 << HERE
sudo cat /proc/net/dev > netaftervm3.txt
sudo cat /proc/diskstats > diskaftervm3.txt
scp netaftervm3.txt vm1:~/logs-trial/query$query/

scp diskaftervm3.txt vm1:~/logs-trial/query$query/
rm -rf netaftervm3.txt diskaftervm3.txt
HERE

ssh vm4 << HERE
sudo cat /proc/net/dev > netaftervm4.txt
sudo cat /proc/diskstats > diskaftervm4.txt
scp netaftervm4.txt vm1:~/logs-trial/query$query/
scp diskaftervm4.txt vm1:~/logs-trial/query$query/
rm -rf netaftervm4.txt diskaftervm4.txt
HERE
