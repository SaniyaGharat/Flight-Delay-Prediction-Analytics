#!/usr/bin/env bash
# ==============================================================================
# HDFS Pseudo-Distributed Mode Setup Script for WSL2 (Ubuntu 22.04 LTS)
# Installs Java 17, SSH, Hadoop 3.3.6 (~/hadoop), applies configuration,
# formats NameNode, and launches HDFS services.
# ==============================================================================
set -euo pipefail

# Normalize USER and HOME for WSL sessions
export USER="$(id -un)"
export HOME="$(getent passwd "$USER" | cut -d: -f6)"
cd "$HOME"

echo "======================================================================"
echo " [PHASE 0] HDFS Setup in WSL2 for user: $USER (HOME=$HOME)"
echo "======================================================================"

# ------------------------------------------------------------------------------
# 1. Package Dependencies: OpenJDK 17, SSH Server, pdsh, rsync
# ------------------------------------------------------------------------------
echo -e "\n[STEP 1/6] Installing OpenJDK 17, OpenSSH Server, and pdsh..."
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jdk openssh-server pdsh rsync curl wget

# Ensure host keys exist and start SSH service
sudo ssh-keygen -A
sudo service ssh start || sudo /etc/init.d/ssh start

# ------------------------------------------------------------------------------
# 2. Passwordless SSH Configuration to localhost
# ------------------------------------------------------------------------------
echo -e "\n[STEP 2/6] Configuring Passwordless SSH to localhost..."
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"

if [ ! -f "$HOME/.ssh/id_rsa" ]; then
    ssh-keygen -t rsa -P "" -f "$HOME/.ssh/id_rsa"
fi

if [ -f "$HOME/.ssh/authorized_keys" ]; then
    if ! grep -q -F "$(cat "$HOME/.ssh/id_rsa.pub")" "$HOME/.ssh/authorized_keys"; then
        cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
    fi
else
    cat "$HOME/.ssh/id_rsa.pub" >> "$HOME/.ssh/authorized_keys"
fi
chmod 600 "$HOME/.ssh/authorized_keys"

# Ensure SSH client doesn't prompt for strict host key verification on localhost
cat << 'EOF' > "$HOME/.ssh/config"
Host localhost 127.0.0.1 0.0.0.0
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    LogLevel ERROR
EOF
chmod 600 "$HOME/.ssh/config"

# Test SSH connection
ssh -o BatchMode=yes localhost "echo 'SSH to localhost: OK'"

# ------------------------------------------------------------------------------
# 3. Download and Extract Hadoop 3.3.6 to ~/hadoop
# ------------------------------------------------------------------------------
HADOOP_DIR="$HOME/hadoop"
echo -e "\n[STEP 3/6] Setting up Hadoop 3.3.6 in $HADOOP_DIR..."

if [ ! -d "$HADOOP_DIR" ]; then
    HADOOP_TAR="$HOME/hadoop-3.3.6.tar.gz"
    if [ ! -f "$HADOOP_TAR" ]; then
        echo "Downloading Apache Hadoop 3.3.6 from fast mirror (dlcdn.apache.org)..."
        if ! wget -q --show-progress -O "$HADOOP_TAR" "https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"; then
            echo "Fast mirror failed. Falling back to archive.apache.org..."
            wget -q --show-progress -O "$HADOOP_TAR" "https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz"
        fi
    fi
    echo "Extracting Hadoop 3.3.6 to $HOME/hadoop..."
    tar -xzf "$HADOOP_TAR" -C "$HOME"
    mv "$HOME/hadoop-3.3.6" "$HADOOP_DIR"
    rm -f "$HADOOP_TAR"
else
    echo "Hadoop directory already exists at $HADOOP_DIR"
fi

# ------------------------------------------------------------------------------
# 4. Environment Variables in ~/.bashrc and hadoop-env.sh
# ------------------------------------------------------------------------------
echo -e "\n[STEP 4/6] Configuring environment variables..."
JAVA_HOME_PATH="/usr/lib/jvm/java-17-openjdk-amd64"

# Ensure hadoop-env.sh has explicit JAVA_HOME, PDSH, and SSH options
HADOOP_ENV="$HADOOP_DIR/etc/hadoop/hadoop-env.sh"
if ! grep -q "export JAVA_HOME=" "$HADOOP_ENV"; then
    echo "export JAVA_HOME=$JAVA_HOME_PATH" >> "$HADOOP_ENV"
else
    sed -i "s|^#* *export JAVA_HOME=.*|export JAVA_HOME=$JAVA_HOME_PATH|" "$HADOOP_ENV"
fi

if ! grep -q "export PDSH_RCMD_TYPE=" "$HADOOP_ENV"; then
    cat << 'EOF' >> "$HADOOP_ENV"
export PDSH_RCMD_TYPE=ssh
export HADOOP_SSH_OPTS="-o StrictHostKeyChecking=no"
EOF
fi

if ! grep -q "HADOOP_HOME" "$HOME/.bashrc"; then
    cat << EOF >> "$HOME/.bashrc"

# Apache Hadoop & Java Environment
export JAVA_HOME=$JAVA_HOME_PATH
export HADOOP_HOME=\$HOME/hadoop
export HADOOP_INSTALL=\$HADOOP_HOME
export HADOOP_MAPRED_HOME=\$HADOOP_HOME
export HADOOP_COMMON_HOME=\$HADOOP_HOME
export HADOOP_HDFS_HOME=\$HADOOP_HOME
export YARN_HOME=\$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_HOME/lib/native
export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin
export PDSH_RCMD_TYPE=ssh
EOF
fi

export JAVA_HOME="$JAVA_HOME_PATH"
export HADOOP_HOME="$HADOOP_DIR"
export PATH="$PATH:$HADOOP_DIR/bin:$HADOOP_DIR/sbin"
export PDSH_RCMD_TYPE=ssh

# ------------------------------------------------------------------------------
# 5. Apply core-site.xml and hdfs-site.xml
# ------------------------------------------------------------------------------
echo -e "\n[STEP 5/6] Applying HDFS cluster configurations..."

# Create core-site.xml
cat << EOF > "$HADOOP_DIR/etc/hadoop/core-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
        <description>Default HDFS NameNode IPC URI</description>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>$HOME/hadoop_tmp</value>
        <description>Hadoop temporary directory</description>
    </property>
</configuration>
EOF

# Create hdfs-site.xml
cat << EOF > "$HADOOP_DIR/etc/hadoop/hdfs-site.xml"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
        <description>Single-node pseudo-distributed replication factor</description>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file://$HOME/hadoop_data/hdfs/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file://$HOME/hadoop_data/hdfs/datanode</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
        <description>NameNode Web UI endpoint</description>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
</configuration>
EOF

mkdir -p "$HOME/hadoop_tmp" "$HOME/hadoop_data/hdfs/namenode" "$HOME/hadoop_data/hdfs/datanode"

# ------------------------------------------------------------------------------
# 6. Format NameNode & Start HDFS
# ------------------------------------------------------------------------------
echo -e "\n[STEP 6/6] Formatting NameNode and starting DFS daemons..."

if [ ! -d "$HOME/hadoop_data/hdfs/namenode/current" ]; then
    echo "Executing: hdfs namenode -format -force -nonInteractive"
    "$HADOOP_DIR/bin/hdfs" namenode -format -force -nonInteractive
else
    echo "NameNode already formatted. Skipping format."
fi

# Stop any running daemons first
"$HADOOP_DIR/sbin/stop-dfs.sh" 2>/dev/null || true

# Start HDFS daemons (NameNode, DataNode, SecondaryNameNode)
"$HADOOP_DIR/sbin/start-dfs.sh"

echo "Waiting for HDFS daemons to initialize..."
sleep 6

echo -e "\n======================================================================"
echo " [SUCCESS] HDFS is running!"
echo " Web UI: http://localhost:9870"
echo "======================================================================"
