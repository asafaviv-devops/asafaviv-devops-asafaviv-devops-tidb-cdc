#!/bin/bash
# Backup script for TiDB CDC

BACKUP_DIR="/backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "Starting backup..."

# Backup TiDB
docker exec tidb-server /usr/bin/dumpling \
  -u root \
  -P 4000 \
  -o "$BACKUP_DIR/tidb"

# Backup Elasticsearch indices
docker exec elasticsearch \
  curl -X PUT "localhost:9200/_snapshot/backup/snapshot_$(date +%Y%m%d)" \
  -H 'Content-Type: application/json' \
  -d'{"indices": "tidb-cdc-events"}'

# Backup configurations
cp -r /home/tidb-cdc-v2 "$BACKUP_DIR/config"

echo "Backup completed: $BACKUP_DIR"
