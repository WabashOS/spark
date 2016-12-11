#!/bin/bash

# Copies the jars from the remoteBuf library into maven
# Required any time you rebuild remoteBuf

RMEM_DIR=./external/remotebuf

if ! [ -e $RMEM_DIR ]
then
  echo "Remote Mem directory doesn't exist"
  echo "Usage: ./copy_jar.sh /path/to/remotebuf/"
  exit 1
fi

RMEM_JAR=${RMEM_DIR}/remoteMem.jar
JAVACPP_JAR=${RMEM_DIR}/javacpp/target/javacpp.jar

if [ ! -e ${RMEM_JAR} -o ! -e ${JAVACPP_JAR} ] 
then
  echo "Missing JARs (did you build them?)"
  exit 1
fi

mvn install:install-file -DlocalRepositoryPath=rmemLib -DcreateChecksum=true -Dpackaging=jar -Dfile=${RMEM_JAR} -DgroupId=ucb.remotebuf -DartifactId=remoteMemCore -Dversion=0.1

mvn install:install-file -DlocalRepositoryPath=rmemLib -DcreateChecksum=true -Dpackaging=jar -Dfile=${JAVACPP_JAR} -DgroupId=org.bytedeco.javacpp  -DartifactId=remoteMemCpp -Dversion=0.1

cp ${RMEM_DIR}/libRemoteBuf.so ./rmemLib/
cp ${RMEM_DIR}/ucb/remotebuf/linux-x86_64/libjniRemoteBuf.so ./rmemLib
