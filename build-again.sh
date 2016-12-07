#!/bin/bash

./build/mvn --global-settings ../settings.xml -Pyarn -Phive -Phive-thriftserver -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests -T 16 package
