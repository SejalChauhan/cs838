set -e

# Pass in two parameters: First - query-id and Second - platform (mr or tez)
query=$1
platform=$2
reducers=$3
parallel=$4
slowstart=$5
containerJvm="-Xmx4600m"
containerSize=4800

echo "Query ($query)"
logFolder=query$query$platform
mkdir -p $logFolder
echo "created folder $logFolder" 

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

echo "Dropped caches successfully" 

#clearing hadoop fs history folder
echo "clearing hadoop fs history folder"
if [ "$platform" = "mr" ]; then
    hadoop fs -rm -r -f /tmp/hadoop-yarn/staging/history/*
else
    hadoop fs -rm -r -f /tmp/tez-history/*
fi


#collecting the network and disk bandwidth stats before
sudo cat /proc/net/dev > $logFolder/netbeforevm1.txt
sudo cat /proc/diskstats > $logFolder/diskbeforevm1.txt

ssh vm2 << HERE
sudo cat /proc/net/dev > netbeforevm2.txt
sudo cat /proc/diskstats > diskbeforevm2.txt
scp netbeforevm2.txt vm1:~/scripts/cs838/$logFolder/
scp diskbeforevm2.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netbeforevm2.txt diskbeforevm2.txt
HERE

ssh vm3 << HERE
sudo cat /proc/net/dev > netbeforevm3.txt
sudo cat /proc/diskstats > diskbeforevm3.txt
scp netbeforevm3.txt vm1:~/scripts/cs838/$logFolder/
scp diskbeforevm3.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netbeforevm3.txt diskbeforevm3.txt
HERE

ssh vm4 << HERE
sudo cat /proc/net/dev > netbeforevm4.txt
sudo cat /proc/diskstats > diskbeforevm4.txt
scp netbeforevm4.txt vm1:~/scripts/cs838/$logFolder/
scp diskbeforevm4.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netbeforevm4.txt diskbeforevm4.txt
HERE

#run the desired query

cd ~/workload/hive-tpcds-tpch-workload
if [ "$platform" = "mr" ]; then
        (hive --hiveconf hive.execution.engine=mr --hiveconf mapred.reduce.tasks=$reducers --hiveconf mapreduce.reduce.shuffle.parallelcopies=$parallel --hiveconf mapreduce.job.reduce.slowstart.completedmaps=$slowstart --hiveconf hive.cbo.enable=true -f sample-queries-tpcds/query$query.sql --database tpcds_text_db_1_50) 2> output/query_$platform.out
else
        (hive --hiveconf hive.execution.engine=$platform --hiveconf hive.cbo.enable=true --hiveconf hive.tez.container.size=$containerSize --hiveconf hive.tez.java.opts=$containerJvm  --hiveconf hive.cbo.enable=true -f sample-queries-tpcds/query$query.sql --database tpcds_text_db_1_50) 2> output/query_$platform.out
fi

cd -
mv ~/workload/hive-tpcds-tpch-workload/output/query_$platform.out $logFolder/

#copying logs jhist file locally
#if [ ! hadoop fs -d "/tmp/hadoop-yarn/staging/history/done_intermediate" ]; then
sleep 120 
if [ "$platform" = "mr" ]; then
        hadoop fs -get /tmp/hadoop-yarn/staging/history/ $logFolder/
else
        hadoop fs -get /tmp/tez-history/ $logFolder
fi


#collecting the network and disk bandwidth stats before
sudo cat /proc/net/dev > $logFolder/netaftervm1.txt
sudo cat /proc/diskstats > $logFolder/diskaftervm1.txt

ssh vm2 << HERE
sudo cat /proc/net/dev > netaftervm2.txt
sudo cat /proc/diskstats > diskaftervm2.txt
scp netaftervm2.txt vm1:~/scripts/cs838/$logFolder/
scp diskaftervm2.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netaftervm2.txt diskaftervm2.txt
HERE

ssh vm3 << HERE
sudo cat /proc/net/dev > netaftervm3.txt
sudo cat /proc/diskstats > diskaftervm3.txt
scp netaftervm3.txt vm1:~/scripts/cs838/$logFolder/

scp diskaftervm3.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netaftervm3.txt diskaftervm3.txt
HERE

ssh vm4 << HERE
sudo cat /proc/net/dev > netaftervm4.txt
sudo cat /proc/diskstats > diskaftervm4.txt
scp netaftervm4.txt vm1:~/scripts/cs838/$logFolder/
scp diskaftervm4.txt vm1:~/scripts/cs838/$logFolder/
rm -rf netaftervm4.txt diskaftervm4.txt
HERE
