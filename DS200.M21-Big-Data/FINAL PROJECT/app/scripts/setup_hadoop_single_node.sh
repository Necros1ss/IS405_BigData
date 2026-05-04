#!/usr/bin/env bash
set -euo pipefail

# setup_hadoop_single_node.sh
# Usage: sudo bash setup_hadoop_single_node.sh
# Installs OpenJDK, downloads Hadoop, configures a pseudo-distributed single-node
# Hadoop 3.x installation under user 'hadoop'. Run on Lubuntu VM.

HADOOP_VER="3.3.6"
HADOOP_TGZ="hadoop-${HADOOP_VER}.tar.gz"
HADOOP_URL="https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VER}/${HADOOP_TGZ}"
HADOOP_INSTALL_DIR="/opt/hadoop-${HADOOP_VER}"
HADOOP_SYMLINK="/opt/hadoop"
HADOOP_USER="hadoop"

if [[ $(id -u) -ne 0 ]]; then
  echo "Please run as root (sudo)."
  exit 1
fi

apt update
apt install -y openjdk-11-jdk ssh rsync wget tar

# Create hadoop user if missing
if ! id -u ${HADOOP_USER} >/dev/null 2>&1; then
  adduser --disabled-password --gecos "" ${HADOOP_USER}
  usermod -aG sudo ${HADOOP_USER}
fi

# Download and extract Hadoop
if [[ ! -d "${HADOOP_INSTALL_DIR}" ]]; then
  echo "Downloading Hadoop ${HADOOP_VER}..."
  wget -q ${HADOOP_URL} -O /tmp/${HADOOP_TGZ}
  tar -xzf /tmp/${HADOOP_TGZ} -C /opt
  ln -sfn ${HADOOP_INSTALL_DIR} ${HADOOP_SYMLINK}
  chown -R ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_INSTALL_DIR}
fi

HADOOP_HOME=${HADOOP_SYMLINK}
HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# Add environment variables to hadoop user's .bashrc
HADOOP_ENV_FILE="/home/${HADOOP_USER}/.bashrc"
grep -q "HADOOP_HOME=${HADOOP_HOME}" ${HADOOP_ENV_FILE} 2>/dev/null || cat >> ${HADOOP_ENV_FILE} <<'EOF'
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
EOF

chown ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_ENV_FILE}

# Setup passwordless SSH for hadoop user
sudo -u ${HADOOP_USER} bash -c '
  mkdir -p ~/.ssh
  if [[ ! -f ~/.ssh/id_rsa ]]; then
    ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
  fi
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
  chmod 600 ~/.ssh/authorized_keys
'

# Create HDFS directories
mkdir -p /home/${HADOOP_USER}/hdfs/namenode
mkdir -p /home/${HADOOP_USER}/hdfs/datanode
chown -R ${HADOOP_USER}:${HADOOP_USER} /home/${HADOOP_USER}/hdfs

# Write minimal configuration files (overwrites existing config)
sudo -u ${HADOOP_USER} bash -c "cat > ${HADOOP_CONF_DIR}/core-site.xml <<'XML'
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
  </property>
</configuration>
XML
"

sudo -u ${HADOOP_USER} bash -c "cat > ${HADOOP_CONF_DIR}/hdfs-site.xml <<'XML'
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/${HADOOP_USER}/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/${HADOOP_USER}/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.blocksize</name>
    <value>134217728</value>
  </property>
</configuration>
XML
"

sudo -u ${HADOOP_USER} bash -c "cat > ${HADOOP_CONF_DIR}/mapred-site.xml <<'XML'
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
XML
"

sudo -u ${HADOOP_USER} bash -c "cat > ${HADOOP_CONF_DIR}/yarn-site.xml <<'XML'
<configuration>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>3072</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>2</value>
  </property>
</configuration>
XML
"

# Ensure hadoop-env.sh has correct JAVA_HOME
sed -i "/^export JAVA_HOME=/d" ${HADOOP_CONF_DIR}/hadoop-env.sh
echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" >> ${HADOOP_CONF_DIR}/hadoop-env.sh
chown ${HADOOP_USER}:${HADOOP_USER} ${HADOOP_CONF_DIR}/hadoop-env.sh

echo "Formatting NameNode (will skip if already formatted)..."
if [[ ! -f /home/${HADOOP_USER}/hdfs/namenode/current/VERSION ]]; then
  sudo -u ${HADOOP_USER} bash -c "${HADOOP_HOME}/bin/hdfs namenode -format -force"
else
  echo "NameNode already formatted."
fi

echo "Starting HDFS and YARN as ${HADOOP_USER}..."
sudo -u ${HADOOP_USER} bash -c "${HADOOP_HOME}/sbin/start-dfs.sh"
sudo -u ${HADOOP_USER} bash -c "${HADOOP_HOME}/sbin/start-yarn.sh"

echo "Done. Check Java processes with 'jps' and NameNode UI at http://localhost:9870 and YARN at http://localhost:8088"
