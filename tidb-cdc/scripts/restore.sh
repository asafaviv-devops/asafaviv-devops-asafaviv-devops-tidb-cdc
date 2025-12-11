#!/bin/bash
# Restore script for TiDB CDC

if [ -z "$1" ]; then
  echo "Usage: $0 <backup_id>"
  exit 1
fi

BACKUP_DIR="/backups/$1"

if [ ! -d "$BACKUP_DIR" ]; then
  echo "Backup not found: $BACKUP_DIR"
  exit 1
fi

echo "Restoring from: $BACKUP_DIR"

# Restore TiDB
docker exec tidb-server /usr/bin/tidb-lightning \
  -d "$BACKUP_DIR/tidb"

# Restore Elasticsearch
docker exec elasticsearch \
  curl -X POST "localhost:9200/_snapshot/backup/snapshot_$1/_restore"

echo "Restore completed"
