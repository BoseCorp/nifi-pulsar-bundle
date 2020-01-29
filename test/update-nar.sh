#!/bin/bash
docker cp ../nifi-pulsar-bose-nar/target/nifi-pulsar-bose-nar-1.9.0.nar nifi:/tmp/
docker exec -u root nifi /bin/chown nifi:nifi /tmp/nifi-pulsar-bose-nar-1.9.0.nar
docker exec -u root nifi /bin/chmod 664 /tmp/nifi-pulsar-bose-nar-1.9.0.nar
docker exec -u root nifi /bin/mv /tmp/nifi-pulsar-bose-nar-1.9.0.nar /opt/nifi/nifi-current/extensions/
echo "You must restart the Nifi container."