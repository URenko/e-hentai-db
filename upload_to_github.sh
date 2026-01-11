#!/bin/bash
set -e

DB_USER="root"
DB_NAME="e-hentai-db"
BACKUP_FILE="nightly.sql.zstd"
REPO="URenko/e-hentai-db"
TAG="nightly"
GITHUB_TOKEN="$GITHUB_TOKEN"

# === 1. 获取 Release ID ===
RELEASE_ID=$(curl -s \
  -H "Authorization: token $GITHUB_TOKEN" \
  https://api.github.com/repos/$REPO/releases/tags/$TAG | jq -r '.id')

if [ "$RELEASE_ID" == "null" ] || [ -z "$RELEASE_ID" ]; then
  echo "❌ Release '$TAG' not found!"
  exit 1
fi

# === 2. 删除旧 Asset ===
ASSET_ID=$(curl -s \
  -H "Authorization: token $GITHUB_TOKEN" \
  https://api.github.com/repos/$REPO/releases/$RELEASE_ID/assets | jq -r ".[] | select(.name==\"$BACKUP_FILE\") | .id")

if [ -n "$ASSET_ID" ]; then
  echo "🧹 Deleting old asset..."
  curl -s -X DELETE \
    -H "Authorization: token $GITHUB_TOKEN" \
    https://api.github.com/repos/$REPO/releases/assets/$ASSET_ID > /dev/null
fi

# === 3. 流式 mysqldump + zstd 上传 ===
echo "🚀 Streaming backup and uploading..."
mysqldump -u $DB_USER $DB_NAME | zstd -v -c | \
curl -s -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Content-Type: application/zstd" \
  --data-binary @- \
  "https://uploads.github.com/repos/$REPO/releases/$RELEASE_ID/assets?name=$BACKUP_FILE"

echo "✅ Upload complete!"
