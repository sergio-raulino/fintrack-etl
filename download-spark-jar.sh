mkdir -p ./spark/jars
cd ./spark/jars

# AWS SDK
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.734/aws-java-sdk-bundle-1.12.734.jar

# Hadoop 3.3.6
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# Hadoop AWS 3.3.6
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar

# Spark Hive 3.5.4 (Scala 2.12)
wget https://repo1.maven.org/maven2/org/apache/spark/spark-hive_2.12/3.5.4/spark-hive_2.12-3.5.4.jar

# Iceberg (Spark Runtime 3.5 — Scala 2.12)
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar

# StAX & Woodstox
# stax2-api 4.2.1 (groupId correto é org.codehaus.woodstox)
wget https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar
wget https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/6.5.1/woodstox-core-6.5.1.jar
