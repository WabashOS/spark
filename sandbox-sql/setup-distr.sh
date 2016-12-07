

../sbin/start-master.sh
../sbin/slaves.sh rm -r /scratch/sagark/spark-shuffle
../sbin/slaves.sh scp -r f16:/scratch/sagark/spark-shuffle /scratch/sagark/
../sbin/start-slaves.sh
