#!/bin/bash
rcServer="10.232.36.210:5632"
appKey="tappkey00002"
localIp="10.13.88.118"
phpbin=/home/admin/local/bin/

#test creatDir
i=1
echo "<=======================case create_dir start=====================================>" > log.test_01_createDir
while [ $i -lt 11 ]
do
${phpbin}php -q test_01_createDir.php $rcServer $appKey $localIp $i >> log.test_01_createDir 2>&1
i=`expr $i + 1`
done
echo "<========================case create_dir end=====================================>" >> log.test_01_createDir

#test createFile
i=1
echo "<======================case create_file start====================================>" > log.test_02_createFile
while [ $i -lt 13 ]
do
${phpbin}php -q test_02_createFile.php $rcServer $appKey $localIp $i  >> log.test_02_createFile 2>&1
i=`expr $i + 1`
done
echo "<======================case create_file end=====================================>" >> log.test_02_createFile

#test mvDir
i=1
echo "<==================case test_mvDir start=======================>" > log.test_03_mvDir
while [ $i -lt 19 ]
do
${phpbin}php -q test_03_mvDir.php $rcServer $appKey $localIp $i >> log.test_03_mvDir 2>&1
i=`expr $i + 1`
done
echo "<===================case test_mvDir end=======================>" >> log.test_03_mvDir

#test mvFile
i=1
echo "<==================case test_mvFile start=======================>" > log.test_04_mvFile
while [ $i -lt 15 ]
do
${phpbin}php -q test_04_mvFile.php $rcServer $appKey $localIp $i >> log.test_04_mvFile 2>&1
i=`expr $i + 1`
done
echo "<===================case test_mvFile end=======================>" >> log.test_04_mvFile

#test rmDir
i=1
echo "<==================case test_rmDir start=======================>" > log.test_05_rmDir
while [ $i -lt 11 ]
do
${phpbin}php -q test_05_rmDir.php $rcServer $appKey $localIp $i >> log.test_05_rmDir 2>&1
i=`expr $i + 1`
done
echo "<===================case test_rmDir end=======================>" >> log.test_05_rmDir

#test rmFile
i=1
echo "<==================case test_rmFile start=======================>" > log.test_06_rmFile
while [ $i -lt 9 ]
do
${phpbin}php -q test_06_rmFile.php $rcServer $appKey $localIp $i >> log.test_06_rmFile 2>&1
i=`expr $i + 1`
done
echo "<===================case test_rmFile end=======================>" >> log.test_06_rmFile

#test lsDir
i=1
echo "<==================case test_lsDir start=======================>" > log.test_07_lsDir
while [ $i -lt 7 ]
do
${phpbin}php -q test_07_lsDir.php $rcServer $appKey $localIp $i >> log.test_07_lsDir 2>&1
i=`expr $i + 1`
done
echo "<===================case test_lsDir end=======================>" >> log.test_07_lsDir

#test fopen
i=1
echo "<==================case test_fopen start=======================>" > log.test_09_fopen
while [ $i -lt 11 ]
do
${phpbin}php -q test_09_fopen.php $rcServer $appKey $localIp $i >> log.test_09_fopen 2>&1
i=`expr $i + 1`
done
echo "<===================case test_fopen end=======================>" >> log.test_09_fopen

#test pwrite                                                                                            
i=1
echo "<==================case test_pwrite start=======================>" > log.test_10_pwrite
while [ $i -lt 6 ]
do
${phpbin}php -q test_10_pwrite.php $rcServer $appKey $localIp $i >> log.test_10_pwrite 2>&1
i=`expr $i + 1`
done
echo "<===================case test_pwrite end=======================>" >> log.test_10_pwrite

#test pread
i=1
echo "<==================case test_pread start=======================>" > log.test_11_pread
while [ $i -lt 6 ]
do
${phpbin}php -q test_11_pread.php $rcServer $appKey $localIp $i >> log.test_11_pread 2>&1
i=`expr $i + 1`
done
echo "<===================case test_pread end=======================>" >> log.test_11_pread

#test fstat
i=1
echo "<==================case test_fstat start=======================>" > log.test_12_fstat
while [ $i -lt 8 ]
do
${phpbin}php -q test_12_fstat.php $rcServer $appKey $localIp $i >> log.test_12_fstat 2>&1
i=`expr $i + 1`
done
echo "<===================case test_fstat end=======================>" >> log.test_12_fstat

#test read
i=1
echo "<==================case test_read start=======================>" > log.test_13_read
while [ $i -lt 4 ]
do
${phpbin}php -q test_13_read.php $rcServer $appKey $localIp $i >> log.test_13_read 2>&1
i=`expr $i + 1`
done
echo "<===================case test_read end=======================>" >> log.test_13_read

#test stat
i=1
echo "<==================case test_stat start=======================>" > log.test_14_stat
while [ $i -lt 3 ]
do
${phpbin}php -q test_14_stat.php $rcServer $appKey $localIp $i >> log.test_14_stat 2>&1
i=`expr $i + 1`
done
echo "<===================case test_stat end=======================>" >> log.test_14_stat

#test unlink
i=1
echo "<==================case test_unlink start=======================>" > log.test_15_unlink
while [ $i -lt 10 ]
do
${phpbin}php -q test_15_unlink.php $rcServer $appKey $localIp $i >> log.test_15_unlink 2>&1
i=`expr $i + 1`
done
echo "<===================case test_unlink end=======================>" >> log.test_15_unlink

#test write
i=1
echo "<==================case test_write start=======================>" > log.test_16_write
while [ $i -lt 12 ]
do
${phpbin}php -q test_16_write.php $rcServer $appKey $localIp $i >> log.test_16_write 2>&1
i=`expr $i + 1`
done
echo "<===================case test_write end=======================>" >> log.test_16_write


rm log/ -rf
mkdir log
mv log.test_* log/
