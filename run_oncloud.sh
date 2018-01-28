mvn compile exec:java -Dexec.mainClass=com.teowaki.RecordsPerStartStationWithDiscardedAndStations -Dexec.args="--fileSource=gs://df_j/examples/20170512.csv --stationsFileSource=gs://df_j/examples/bikeStations.csv --outputPath=gs://df_j/output/valid.csv --invalidRecordsOutputPath=gs://df_j/output/invalid.csv --runner=DataflowRunner --project=qwiklabs-gcp-1825e2c825621a65 --stagingLocation=gs://df_j/staging_df"