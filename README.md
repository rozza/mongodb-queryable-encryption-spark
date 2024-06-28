## Using MongoDB's Queryable Encryption with Spark

This is a demo of based on the
[ClientSideEncryptionAutoEncryptionSettingsTour](https://github.com/mongodb/mongo-java-driver/blob/master/driver-sync/src/examples/tour/ClientSideEncryptionAutoEncryptionSettingsTour.java). Using Spark to run the code.

### Requirements

  - osx
  - Spark 3.5.1 compiled with Scala 2.12
  - MongoDB running on `mongodb://localhost:27107`
  - `mongocryptd` on the path

### Running

```shell
# Run via gradle
./gradlew run 

# Build
./gradlew build

# Run via spark-submit
./spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.mongodb:mongodb-crypt:1.8.0 \
   --class demo.App app/build/libs/app.jar 

```
