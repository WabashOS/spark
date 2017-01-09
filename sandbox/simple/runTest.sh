#!/bin/bash
set -x
# ../../bin/spark-submit --class="SimpleApp" --master spark://$(hostname):7077 target/scala-2.11/simple-project_2.11-1.0.jar
../../bin/spark-submit --class="SimpleApp" --master local[4] target/scala-2.11/simple-project_2.11-1.0.jar
