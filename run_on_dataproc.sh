 #create cluster and connect following instructions at https://github.com/davorbonaci/beam-portability-demo
 #
 gcloud dataproc clusters create gaming-spark \
     --image-version=1.0 \
     --zone=us-central1-f \
     --num-workers=2 \
     --worker-machine-type=n1-standard-2 \
     --master-machine-type=n1-standard-2 \
     --worker-boot-disk-size=100gb \
     --master-boot-disk-size=100gb

#To view the Apache Spark UI, in another terminal:

    # gcloud compute ssh --zone=us-central1-f --ssh-flag="-D 1080" --ssh-flag="-N" --ssh-flag="-n" gaming-spark-m

#Launch magic Google Chrome window and, if applicable, set BeyondCorp to System/Alternative:
    #  /opt/google/chrome/chrome --proxy-server="socks5://localhost:1080"     --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost"     --user-data-dir=/tmp/

#Open the UI:

    #http://gaming-spark-m:18080/

# Submit the job to the cluster:

    # mvn clean package -Pspark-runner
    # gcloud dataproc jobs submit spark  --cluster gaming-spark     --properties spark.default.parallelism=4 --class com.teowaki.RecordsPerStartStationWithDiscardedAndStations --jars ./target/tfl-demo-bundled-spark.jar --  --runner=spark --fileSource=gs://df_j/examples/20170512.csv --stationsFileSource=gs://df_j/examples/bikeStations.csv --outputPath=gs://df_j/output_spark/valid.csv --invalidRecordsOutputPath=gs://df_j/output_spark/invalid.csv