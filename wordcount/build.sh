javac -classpath $HADOOP_HOME/share/hadoop/common/hadoop-common-2.2.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.2.0.jar:$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar:$HADOOP_HOME/share/hadoop/common/lib/hadoop-annotations-2.2.0.jar WordCount.java
jar cvf wordcount.jar *.class
