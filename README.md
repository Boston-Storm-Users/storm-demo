storm-demo

run in local mode for 2 minutes:
mvn test

build jar:
mvn package -DskipTests

deploy to local cluster:
storm-0.8.2/bin/storm jar ./target/storm-demo.jar com.tnorden.storm_demo.trident.topology.TridentDemo