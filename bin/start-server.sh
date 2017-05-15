#!/usr/bin/env bash

bin=`dirname ${0}`
bin=`cd ${bin}; pwd`
basedir=${bin}/..
LIB_DIR=${basedir}/lib
JARS_DIR=${basedir}/jars
LOG_DIR=${basedir}/logs
CONF_DIR=${basedir}/conf
RUN_DIR=${basedir}/run
NATIVE_OPTS="-Djava.library.path=${basedir}/native"
cd ${basedir}

version=0.3.0

export SPARK_HOME=/apps/svr/spark
export HADOOP_CONF_DIR=${CONF_DIR}
export SPARK_SUBMIT_LIBRARY_PATH=${basedir}/native:$SPARK_SUBMIT_LIBRARY_PATH

if [ ! -d ${LOG_DIR} ]; then
  mkdir -p ${LOG_DIR}
fi
if [ ! -d ${RUN_DIR} ]; then
  mkdir -p ${RUN_DIR}
fi

${SPARK_HOME}/bin/spark-submit \
  --conf spark.driver.extraJavaOptions="${NATIVE_OPTS}" \
  --conf spark.yarn.executor.memoryOverhead=768 \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=800 \
  --driver-class-path ${CONF_DIR}:${JARS_DIR}/*:${LIB_DIR}/phoenix-GIO-2.1.0-4.7.0-client-spark.jar:${JARS_DIR}/elasticsearch-spark-20_2.11-5.2.2.jar \
  --jars ${JARS_DIR}/api-${version}.jar,${JARS_DIR}/jobs-${version}.jar,${JARS_DIR}/bitmap-0.6.26-GIO1.7.jar,${JARS_DIR}/backend-commons-0.1.0.jar,${LIB_DIR}/phoenix-GIO-2.1.0-4.7.0-client-spark.jar,${JARS_DIR}/elasticsearch-spark-20_2.11-5.2.2.jar \
  --files ${CONF_DIR}/jets3t.properties \
  --master yarn \
  --deploy-mode client \
  --num-executors 10 \
  --executor-cores 1 \
  --driver-memory 2g \
  --executor-memory 2g \
  --class io.growing.offline.Server \
  ${JARS_DIR}/server-${version}.jar $@ \
  > ${LOG_DIR}/offline.log 2>&1 &

echo $! > ${RUN_DIR}/pid

