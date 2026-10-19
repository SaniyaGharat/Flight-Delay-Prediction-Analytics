#!/usr/bin/env bash
set -e

# Restore default skel
cp /etc/skel/.bashrc /home/saniya/.bashrc
cp /etc/skel/.profile /home/saniya/.profile

# Write clean hadoop environment to /etc/profile.d/hadoop.sh
cat << 'EOF' | sudo tee /etc/profile.d/hadoop.sh > /dev/null
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/saniya/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export PDSH_RCMD_TYPE=ssh
EOF
sudo chmod 644 /etc/profile.d/hadoop.sh

# Prepend to /home/saniya/.bashrc so non-interactive shells also get it
cat << 'EOF' > /tmp/env_head
# Apache Hadoop & Java Environment
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/saniya/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export PDSH_RCMD_TYPE=ssh

EOF

cat /tmp/env_head /home/saniya/.bashrc > /tmp/bashrc_merged
mv /tmp/bashrc_merged /home/saniya/.bashrc
chown saniya:saniya /home/saniya/.bashrc /home/saniya/.profile

echo "Environment cleanly restored."
