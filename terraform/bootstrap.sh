#!/bin/bash

# Parses a configuration file put in place by EMR to determine the role of this node
is_master() {
  if [ $(jq '.isMaster' /mnt/var/lib/info/instance.json) = 'true' ]; then
    return 0
  else
    return 1
  fi
}

if is_master; then
    cat <<EOF > /tmp/patch.sh
#!/bin/bash

while [ ! -e "/etc/spark/conf.dist/spark-defaults.conf" ]; do
   sleep 3
done
sed -i 's,\(^.*extraJavaOptions.*\),\1  -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9333 -Dcom.sun.management.jmxremote.rmi.port=9333 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=localhost,' /etc/spark/conf.dist/spark-defaults.conf
EOF

    cat /tmp/patch.sh # log
    chmod ugo+x /tmp/patch.sh
    sudo /tmp/patch.sh &
fi
