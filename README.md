# spark processor template
Spark processors examples to handle data on EMR clusters.
## Local Requirements
To build and package:
```
brew install scala sbt
```
To run spark locally and analyze S3 input and output:
```
brew install spark parquet-tools
```
Although it is possible to submit jobs to a local spark cluster, the applications are designed to be run on an EMR cluster with the appropriate permissions.

## Creating an EMR Cluster
From the AWS console choose EMR the choose 'Create cluster'. Select 'go to advanced options'.  Choose application and 
options as desired.
##### AWS CLI Example
```
aws emr create-cluster --auto-scaling-role EMR_AutoScaling_DefaultRole --termination-protected --applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark Name=Oozie Name=Ganglia Name=Zeppelin Name=Mahout --ec2-attributes '{"KeyName":"user_services_test","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-09217f23","EmrManagedSlaveSecurityGroup":"sg-1c5e0d61","EmrManagedMasterSecurityGroup":"sg-1d5e0d60","AdditionalMasterSecurityGroups":["sg-fdb1a186"]}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.2.0 --log-uri 's3n://idn-processor/logs/' --name 'idn-processor' --instance-groups '[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":200,"VolumeType":"gp2"},"VolumesPerInstance":2}],"EbsOptimized":true},"InstanceGroupType":"MASTER","InstanceType":"m4.4xlarge","Name":"Master - 1"},{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":100,"VolumeType":"gp2"},"VolumesPerInstance":1}],"EbsOptimized":true},"InstanceGroupType":"CORE","InstanceType":"m4.2xlarge","Name":"Core - 2"}]' --scale-down-behavior TERMINATE_AT_INSTANCE_HOUR --region us-east-1
```

## Submit Jobs on EMR Cluster
Log into the EMR Cluster Master node and submit the desired Spark job with the appropriate options.  For example:
```
spark-submit --class processor.app.CassandraConnectorPOC --packages datastax:spark-cassandra-connector:2.0.0-M2-s_2.11 ./apps/processor-spark2-shaded.jar --host <cassandra-ip>
```

## Useful Commands
##### Get Spark Executors IPs:
```
hdfs dfsadmin -report | grep ^Name | cut -f2 -d: | cut -f2 -d' '
```

## EMR Monitoring
Set up a [foxy proxy for EMR](http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-connect-master-node-proxy.html).
##### Relevant UI Links:
* [Spark UI](http://master-public-dns-name:18080/)
* [Hadoop ResourceManager](http://ec2-54-89-175-185.compute-1.amazonaws.com:9026/)
* [Hadoop HDFS NameNode](http://ec2-54-89-175-185.compute-1.amazonaws.com:9101/)
* [Ganglia Metrics Reports](http://ec2-54-89-175-185.compute-1.amazonaws.com/ganglia/)  
* [HBase Interface](http://ec2-54-89-175-185.compute-1.amazonaws.com:60010/master-status)
* [Hue Web Application](http://ec2-54-89-175-185.compute-1.amazonaws.com:8888/)
