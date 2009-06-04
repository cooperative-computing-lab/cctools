#!/bin/sh

export JAVA_HOME=
export HADOOP_HOME=

if [ -z "$JAVA_HOME" -o -z "$HADOOP_HOME" ]; then
    echo 2>&1 "Sorry but JAVA_HOME or HADOOP_HOME is not properly defined so HDFS will not work with parrot."
    exit 1
fi

if [ -z $CLASSPATH ]; then
    CLASSPATH=${JAVA_HOME}/jdk/jre/lib
else
    CLASSPATH=${CLASSPATH}:${JAVA_HOME}/jdk/jre/lib
fi

CLASSPATH=${CLASSPATH}:$(ls ${HADOOP_HOME}/hadoop-*-core.jar | head -n 1)
CLASSPATH=${CLASSPATH}:${HADOOP_HOME}/conf
for f in ${HADOOP_HOME}/lib/*.jar; do
    CLASSPATH=${CLASSPATH}:$f
done
export CLASSPATH

exec parrot $@
