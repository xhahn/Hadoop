
#:<<BLOCK
rm -r data/train/tmp
hadoop fs -rmr /usr/my/data/naivebayes/input/train
hadoop fs -rmr /usr/my/data/naivebayes/output/trainn
hadoop fs -rmr /usr/my/data/naivebayes/output/trainNB
hadoop fs -rmr /usr/my/data/naivebayes/output/train
hadoop fs -rmr /usr/my/data/naivebayes/input/testdata
hadoop fs -rmr /usr/my/data/naivebayes/output/test

hadoop jar Put.jar Put ./data/train hdfs://HadoopMaster:9000/usr/my/data/naivebayes/input/train

hadoop jar ./Calculaten.jar Calculaten /usr/my/data/naivebayes/input/train/trainData.txt /usr/my/data/naivebayes/output/trainn/

hadoop jar ./CalculateNB.jar CalculateNB /usr/my/data/naivebayes/output/trainn/part-00000 /usr/my/data/naivebayes/output/trainNB/

hadoop fs -cp /usr/my/data/naivebayes/output/trainn/part-00000 /usr/my/data/naivebayes/input/train/n.txt

hadoop fs -cp /usr/my/data/naivebayes/output/trainNB/part-00000 /usr/my/data/naivebayes/input/train/NB.txt

hadoop jar ./NBTrain.jar NBTrain /usr/my/data/naivebayes/input/train/n.txt /usr/my/data/naivebayes/output/train/

hadoop jar ./SeqParse.jar SeqParse /home/xhahn/project/naivebayes/data/testdata /usr/my/data/naivebayes/input/testdata 
#BLOCK

hadoop jar ./Test.jar Test /usr/my/data/naivebayes/input/testdata /usr/my/data/naivebayes/output/test/

rm part-00000
hadoop fs -get /usr/my/data/naivebayes/output/test/part-00000 ./
javac Cal.java
java Cal
