#!/bin/bash
JDK_VERSION=16
SCALA_VERSION=280
mvn install:install-file -Dfile=kafka-0.7.2_jdk${JDK_VERSION}_scala${SCALA_VERSION}.jar -DgroupId=kafka -DartifactId=kafka-core_${SCALA_VERSION} -Dversion=0.7.2 -Dpackaging=jar
