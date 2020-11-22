#!/bin/sh

java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=5000  -Dcom.sun.management.jmxremote.rmi.port=5000  -Djava.rmi.server.hostname=192.168.99.100  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -jar ./app.jar --debug