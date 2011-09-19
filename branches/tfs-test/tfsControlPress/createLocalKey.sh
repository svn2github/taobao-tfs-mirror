#!/bin/sh

#rm dir
rm -rf ./localkey

#make dir
mkdir localkey

#create localkey
for((i=1;i<200;i++))
do
 touch ./localkey/$i 
done

exit 0
