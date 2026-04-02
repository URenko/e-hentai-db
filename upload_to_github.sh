#!/bin/bash
set -euo pipefail

DB_PATH="${DB_PATH:-${SQLITE_PATH:-./e-hentai.db}}"
BACKUP_FILE="${BACKUP_FILE:-e-hentai.db.zstd}"
TMP_ASSET_NAME="${TMP_ASSET_NAME:-$(date +%s).db.zstd}"
REPO="${REPO:-${GITHUB_REPOSITORY:-URenko/e-hentai-db}}"
TAG="${TAG:-nightly}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
MIN_BACKUP_BYTES="${MIN_BACKUP_BYTES:-1024}"
TMP_BACKUP="$(mktemp "${TMPDIR:-/tmp}/e-hentai.db.XXXXXX.zstd")"

cleanup() {
  rm -f "$TMP_BACKUP"
}
trap cleanup EXIT

if [ -z "$GITHUB_TOKEN" ]; then
  echo "❌ GITHUB_TOKEN is required"
  exit 1
fi

# === 1. 获取 Release ID ===
RELEASE_ID=$(curl -fsS \
  -H "Authorization: token $GITHUB_TOKEN" \
  https://api.github.com/repos/$REPO/releases/tags/$TAG | jq -r '.id')

if [ "$RELEASE_ID" == "null" ] || [ -z "$RELEASE_ID" ]; then
  echo "❌ Release '$TAG' not found!"
  exit 1
fi

# === 2. 先生成本地备份并校验 ===
echo "📦 Generating local backup..."
if [ ! -f "$DB_PATH" ]; then
  echo "❌ SQLite database not found: $DB_PATH"
  exit 1
fi

zstd -T0 -9 -q -c "$DB_PATH" > "$TMP_BACKUP"

BACKUP_SIZE="$(wc -c < "$TMP_BACKUP" | tr -d '[:space:]')"
if [ -z "$BACKUP_SIZE" ] || [ "$BACKUP_SIZE" -lt "$MIN_BACKUP_BYTES" ]; then
  echo "❌ Backup file is too small: ${BACKUP_SIZE:-0} bytes (min ${MIN_BACKUP_BYTES})"
  exit 1
fi
echo "✅ Backup ready (${BACKUP_SIZE} bytes)"

# === 3. 先上传到临时 Asset 名称 ===
echo "🚀 Uploading backup to temp asset: $TMP_ASSET_NAME"
UPLOAD_RESPONSE="$(curl -fsS -X POST \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Content-Type: application/zstd" \
  --data-binary @"$TMP_BACKUP" \
  "https://uploads.github.com/repos/$REPO/releases/$RELEASE_ID/assets?name=$TMP_ASSET_NAME")"

UPLOADED_ID="$(echo "$UPLOAD_RESPONSE" | jq -r '.id')"
if [ -z "$UPLOADED_ID" ] || [ "$UPLOADED_ID" = "null" ]; then
  echo "❌ Upload failed: $UPLOAD_RESPONSE"
  exit 1
fi

# === 4. 删除旧 Asset ===
ASSET_ID=$(curl -fsS \
  -H "Authorization: token $GITHUB_TOKEN" \
  https://api.github.com/repos/$REPO/releases/$RELEASE_ID/assets | jq -r ".[] | select(.name==\"$BACKUP_FILE\") | .id")

if [ -n "$ASSET_ID" ] && [ "$ASSET_ID" != "null" ]; then
  echo "🧹 Deleting old asset: $BACKUP_FILE"
  curl -fsS -X DELETE \
    -H "Authorization: token $GITHUB_TOKEN" \
    https://api.github.com/repos/$REPO/releases/assets/$ASSET_ID > /dev/null
fi

# === 5. 将临时 Asset 重命名为正式名称 ===
RENAME_PAYLOAD="$(jq -nc --arg name "$BACKUP_FILE" '{name:$name}')"
echo "🏷️ Renaming temp asset to: $BACKUP_FILE"
RENAME_RESPONSE="$(curl -fsS -X PATCH \
  -H "Authorization: token $GITHUB_TOKEN" \
  -H "Content-Type: application/json" \
  -d "$RENAME_PAYLOAD" \
  https://api.github.com/repos/$REPO/releases/assets/$UPLOADED_ID)"

RENAMED_NAME="$(echo "$RENAME_RESPONSE" | jq -r '.name')"
if [ "$RENAMED_NAME" != "$BACKUP_FILE" ]; then
  echo "❌ Rename failed: $RENAME_RESPONSE"
  exit 1
fi

echo "✅ Upload complete!"
