# nifi-pulsar-bundle
The collection of Apache NiFi processors to use with Apache Pulsar. The following subdirectories contain
code from the original Pulsar bundle from streamlio, forked from afire007/nifi-pulsar-bundle (the same code is in the 1.9 branch
of this repo). Note: there are numerous copies of the nifi-pulsar-bundle repo on Github. The official one is most likely https://github.com/openconnectors/nifi-pulsar-bundle)
- nifi-pulsar-client-service
- nifi-pulsar-client-service-api
- nifi-pulsar-client-service-nar
- nifi-pulsar-nar
- nifi-pulsar-processors
Do not change the code in these subdirectories other than to pick up changes from the original repo's 1.9 branch. The
process is to merge changes from the parent repo master or 1.9 branch into the 1.9 branch of this repo, then merge up changes
from the 1.9 branch into this branch.

The purpose of this branch is to add changes to the processor without modifying the original Pulsar processor code. This is done by extending the Pulsar processor classes with code in the `nifi-pulsar-processors-bose` subdirectory and the build configuration in `nifi-pulsar-bose-nar`. The top level pom.xml file was also modified to build the Bose custom code.

## Build Steps
For max compatibility, build with java8
```
$ mvn clean install
```

## Local testing
You can use the scripts in the test/ subdirectory to start Pulsar and Nifi in local docker containers. The streamlio Nifi image used already has the Pulsar bundle included.

## Copying nar files into Nifi container in Kubernetes for testing
```
kubectl -n svc-data-stream-processing-integration cp nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar nifi-0:/opt/nifi/data/extensions/
```
where svc-data-stream-processing-integration is the namespace name and nifi-0 is the pod name.

## Publishing nar files for production
When ready for Nifi to pick up your nar file on startup,
copy the nar file `nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar` to the following S3 location:
```
s3://bose-dp-packages/nifi-nars/
```
Update the config file `https://github.com/BoseCorp/svc-data-processing-nifi/blob/integration/images/nifi/config/files-to-download.yaml` to add the nar file to the list to be downloaded.

To verify that the nar file was copied to the container after startup, use the following command to gain shell access to the pod:
```
kubectl -n svc-data-stream-processing-integration exec nifi-0 -it /bin/bash
```
then run
```
ls -l /opt/nifi/data/extension
```
Note that as a prerequisite, the base Nifi pulsar bundle nar file (`nifi-pulsar-nar/target/nifi-pulsar-nar-1.9.0.nar`) must also be present.