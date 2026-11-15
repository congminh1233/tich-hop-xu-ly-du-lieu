#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR"
case "${1:-}" in
  namenode)
    if [[ ! -f /data/name/current/VERSION ]]; then
      if [[ -n "$(find /data/name -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
        echo 'Refusing to format: NameNode directory is nonempty but VERSION is missing.' >&2
        exit 1
      fi
      hdfs namenode -format -nonInteractive -clusterid CID-it5427-hdfs-v1
    fi
    exec hdfs namenode
    ;;
  datanode) exec hdfs datanode ;;
  *) exec "$@" ;;
esac
