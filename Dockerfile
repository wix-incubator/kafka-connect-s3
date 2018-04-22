FROM confluentinc/cp-kafka-connect:4.0.0
MAINTAINER Or Sher <or.sher@personali.com>

# Add the connector
ADD target/*.jar /etc/kafka-connect/jars/

# Add additional resources
ADD additional_resources/* /etc/additional_resources/
