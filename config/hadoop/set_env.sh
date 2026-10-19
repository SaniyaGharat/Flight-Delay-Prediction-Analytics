#!/usr/bin/env bash
set -e

cat << 'EOF' > /tmp/hadoop_env.sh
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/saniya/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
export PDSH_RCMD_TYPE=ssh
EOF

sudo cp /tmp/hadoop_env.sh /etc/profile.d/hadoop.sh
sudo chmod 644 /etc/profile.d/hadoop.sh

# Also prepend to .bashrc before the non-interactive return check
sed -i '/HADOOP_HOME/d' /home/saniya/.bashrc || true
sed -i '/JAVA_HOME/d' /home/saniya/.bashrc || true
sed -i '/PDSH_RCMD_TYPE/d' /home/saniya/.bashrc || true

# Put at top of .bashrc
cat << 'EOF' > /tmp/bashrc_top
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/home/saniya/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
export PDSH_RCMD_TYPE=ssh

EOF

cat /tmp/bashrc_top /home/saniya/.bashrc > /tmp/new_bashrc
mv /tmp/new_bashrc /home/saniya/.bashrc

echo "Environment successfully configured."
