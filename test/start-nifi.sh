#!/bin/bash
docker run -d -i --name nifi --link pulsar -p 8080:8080 streamlio/nifi
#docker cp ../nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar nifi:/opt/nifi/nifi-current/extensions/
#docker exec -u root nifi /bin/chown nifi:nifi /opt/nifi/nifi-current/extensions/nifi-pulsar-bose-nar-1.9.0.nar
#docker exec -u root nifi /bin/chmod 664 /opt/nifi/nifi-current/extensions/nifi-pulsar-bose-nar-1.9.0.nar
sleep 60
docker cp ../nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar nifi:/tmp/
docker exec -u root nifi /bin/chown nifi:nifi /tmp/nifi-pulsar-bose-nar-1.9.0.nar
docker exec -u root nifi /bin/chmod 664 /tmp/nifi-pulsar-bose-nar-1.9.0.nar
docker exec -u root nifi /bin/mv /tmp/nifi-pulsar-bose-nar-1.9.0.nar /opt/nifi/nifi-current/extensions/
echo "Nifi UI URL: http://localhost:8080/nifi"