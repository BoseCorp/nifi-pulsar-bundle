# nifi-pulsar-bundle
The collection of Apache NiFi processors to use with Apache Pulsar

## Copying nar files into Nifi container in Kubernetes for testing
```
kubectl -n svc-data-stream-processing-integration cp nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar nifi-0:/opt/nifi/data/extensions/
```
where svc-data-stream-processing-integration is the namespace name and nifi-0 is the pod name.

## Publishing nar files for production
When ready for Nifi to pick up your nar file on startup,
copy the nar file to the following S3 location:
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