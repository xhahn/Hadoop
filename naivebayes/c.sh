rm *.class
rm *.jar
#:<<Block
javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ Put.java 
jar -cvf ./Put.jar -C ./ .

javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ Calculaten.java
jar -cvf ./Calculaten.jar -C ./ .

javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ CalculateNB.java
jar -cvf ./CalculateNB.jar -C ./ .

javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ NBTrain.java
jar -cvf ./NBTrain.jar -C ./ .

#javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar:${HADOOP_HOME}/hadoop-mapreduce-client-core-2.5.1.jar:${HADOOP_HOME}/hadoop-tools-1.0.4.jar -d ./ WholeFileInputFormat.java

javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ SeqParse.java
jar -cvf ./SeqParse.jar -C ./ .
#Block
javac -classpath ${HADOOP_HOME}/hadoop-core-1.0.4.jar -d ./ Test.java
jar -cvf ./Test.jar -C ./ .

