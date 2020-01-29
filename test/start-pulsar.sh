#!/bin/bash
docker run -d -i --name pulsar -p 6650:6650 -p 8000:8080  apachepulsar/pulsar-standalone