# Docker デプロイメントガイド

このガイドでは、Docker と Docker Compose を使用して MygramDB をデプロイする方法を説明します。

## クイックスタート

### 1. 前提条件

- Docker 20.10+
- Docker Compose 2.0+

### 2. 環境変数の設定

```bash
# サンプル環境ファイルをコピー
cp .env.example .env

# .env を編集して設定を変更
nano .env
```

**重要:** `.env` 内の以下のデフォルト値を変更してください：

- `MYSQL_ROOT_PASSWORD` - MySQL root パスワード
- `MYSQL_PASSWORD` - MySQL レプリケーションユーザーのパスワード
- `REPLICATION_SERVER_ID` - この MygramDB インスタンスの一意なサーバーID

### 3. サービスの起動（開発環境）

```bash
# すべてのサービスをビルドして起動
docker-compose up -d

# ログを表示
docker-compose logs -f

# ステータスを確認
docker-compose ps
```

### 4. サービスの停止

```bash
# すべてのサービスを停止
docker-compose down

# ボリュームも削除して停止（警告：すべてのデータが削除されます）
docker-compose down -v
```

## 設定

### 環境変数

すべての設定は `.env` ファイルの環境変数で行います：

#### MySQL 設定

```bash
MYSQL_HOST=mysql                    # MySQL ホスト
MYSQL_PORT=3306                     # MySQL ポート
MYSQL_USER=repl_user                # MySQL ユーザー
MYSQL_PASSWORD=your_password        # MySQL パスワード
MYSQL_DATABASE=mydb                 # データベース名
MYSQL_USE_GTID=true                 # GTID ベースのレプリケーションを使用
```

#### テーブル設定

```bash
TABLE_NAME=articles                 # インデックス対象のテーブル
TABLE_PRIMARY_KEY=id                # プライマリキーカラム
TABLE_TEXT_COLUMN=content           # インデックス対象のテキストカラム
TABLE_NGRAM_SIZE=2                  # ASCII用のN-gramサイズ
TABLE_KANJI_NGRAM_SIZE=1            # CJK用のN-gramサイズ
```

#### レプリケーション設定

```bash
REPLICATION_ENABLE=true             # レプリケーションを有効化
REPLICATION_SERVER_ID=12345         # 一意なサーバーID（重要）
REPLICATION_START_FROM=snapshot     # 開始位置: snapshot, latest, または gtid=<UUID:txn>
```

#### メモリ管理

```bash
MEMORY_HARD_LIMIT_MB=8192           # ハードメモリ制限
MEMORY_SOFT_TARGET_MB=4096          # ソフトメモリ目標
MEMORY_NORMALIZE_NFKC=true          # NFKC正規化
MEMORY_NORMALIZE_WIDTH=narrow       # 幅の正規化
```

#### APIサーバー

```bash
API_BIND=0.0.0.0                    # バインドアドレス
API_PORT=11016                      # APIポート
```

#### ロギング

```bash
LOG_LEVEL=info                      # ログレベル: debug, info, warn, error
LOG_FORMAT=json                     # ログ形式: json または text
```

### カスタム設定ファイル

より高度な設定（フィルタ、複数テーブルなど）が必要な場合は、カスタム設定ファイルをマウントできます：

```yaml
# docker-compose.override.yml
version: '3.8'

services:
  mygramdb:
    volumes:
      - ./my-config.yaml:/etc/mygramdb/config.yaml:ro
    environment:
      SKIP_CONFIG_GEN: "true"
    command: ["mygramdb", "-c", "/etc/mygramdb/config.yaml"]
```

または Docker で直接実行：

```bash
# 設定ファイルを作成
cp examples/config-minimal.yaml my-config.yaml
# my-config.yaml を必要に応じて編集

# カスタム設定で実行
docker run -d --name mygramdb \
  -p 11016:11016 \
  -v $(pwd)/my-config.yaml:/etc/mygramdb/config.yaml:ro \
  -e SKIP_CONFIG_GEN=true \
  mygramdb:latest \
  mygramdb -c /etc/mygramdb/config.yaml
```

## 本番環境へのデプロイ

### Docker イメージのビルド

適切なバージョンタグを付けてDockerイメージをビルドする方法：

```bash
# git タグから現在のバージョンを取得
VERSION=$(git describe --tags --abbrev=0 | sed 's/^v//')

# バージョン引数を指定してビルド
docker build --build-arg MYGRAMDB_VERSION=$VERSION -t mygramdb:$VERSION .

# または、バージョンを手動で指定
docker build --build-arg MYGRAMDB_VERSION=1.2.5 -t mygramdb:1.2.5 .

# latest タグを付与
docker tag mygramdb:$VERSION mygramdb:latest
```

**注意:** `MYGRAMDB_VERSION` ビルド引数が指定されない場合、ビルドはバージョン0.0.0を使用するか、ビルドコンテキストに `.git` ディレクトリが存在する場合は git タグから読み取りを試みます。

### ビルド済みイメージの使用

```bash
# GitHub Container Registry から最新イメージを取得
docker pull ghcr.io/libraz/mygram-db:latest

# 本番用 docker-compose ファイルを使用
docker-compose -f docker-compose.prod.yml up -d
```

### 環境設定

1. 本番用 `.env` ファイルを作成：
```bash
cp .env.example .env.prod
nano .env.prod
```

2. 本番用の値を設定：
```bash
# 本番 MySQL 設定
MYSQL_HOST=production-mysql-host
MYSQL_PORT=3306
MYSQL_USER=repl_user
MYSQL_PASSWORD=強力で安全なパスワード

# 本番メモリ設定
MEMORY_HARD_LIMIT_MB=16384
MEMORY_SOFT_TARGET_MB=8192

# 本番 API 設定
API_PORT=11016

# 本番ロギング
LOG_LEVEL=info
LOG_FORMAT=json
```

3. 本番設定で起動：
```bash
docker-compose -f docker-compose.prod.yml --env-file .env.prod up -d
```

### リソース制限

本番用 compose ファイルにはリソース制限が含まれています：

**MySQL:**

- CPU: 2-4 コア
- メモリ: 2-4 GB

**MygramDB:**

- CPU: 4-8 コア
- メモリ: 10-20 GB

ワークロードに応じて `docker-compose.prod.yml` で調整してください。

## データベースの初期化

MySQL コンテナは `support/docker/mysql/init/` 内のスクリプトを自動実行します：

- `01-create-tables.sql` - サンプルテーブルを作成

独自の初期化スクリプトを追加する場合：

```bash
# SQL スクリプトを作成
cat > support/docker/mysql/init/02-my-tables.sql <<EOF
CREATE TABLE my_table (
    id BIGINT PRIMARY KEY,
    content TEXT
);
EOF

# MySQL コンテナを再起動
docker-compose restart mysql
```

## モニタリング

### ログの表示

```bash
# すべてのサービス
docker-compose logs -f

# 特定のサービス
docker-compose logs -f mygramdb

# 最新100行
docker-compose logs --tail=100 mygramdb
```

### ヘルスチェック

```bash
# サービスの健全性を確認
docker-compose ps

# 手動ヘルスチェック
docker exec mygramdb pgrep -x mygramdb
```

### メトリクス

MygramDB は API ポートでメトリクスを公開します。アクセス方法：

```bash
curl http://localhost:11016/metrics
```

## バックアップとリストア

### バックアップ

```bash
# MySQL データのバックアップ
docker exec mygramdb_mysql mysqldump -u root -p${MYSQL_ROOT_PASSWORD} mydb > backup.sql

# MygramDB スナップショットのバックアップ
docker cp mygramdb:/var/lib/mygramdb/dumps ./backup-dumps/
```

### リストア

```bash
# MySQL データのリストア
docker exec -i mygramdb_mysql mysql -u root -p${MYSQL_ROOT_PASSWORD} mydb < backup.sql

# MygramDB スナップショットのリストア
docker cp ./backup-dumps/ mygramdb:/var/lib/mygramdb/dumps/
docker-compose restart mygramdb
```

## トラブルシューティング

### 接続の問題

```bash
# ネットワーク接続を確認
docker-compose exec mygramdb ping mysql

# MySQL 接続を確認
docker-compose exec mygramdb mysql -h mysql -u repl_user -p${MYSQL_PASSWORD} -e "SELECT 1"
```

### 設定の問題

```bash
# バージョンを確認
docker run --rm mygramdb:latest --version

# ヘルプを表示
docker run --rm mygramdb:latest --help

# 設定をテスト
docker-compose exec mygramdb /usr/local/bin/entrypoint.sh test-config

# または環境変数でテスト（未指定の値はデフォルトが使用されます）
docker run --rm -e MYSQL_HOST=testdb -e TABLE_NAME=test mygramdb:latest test-config

# 生成された設定を表示
docker-compose exec mygramdb cat /etc/mygramdb/config.yaml
```

### パフォーマンスの問題

1. リソース使用状況を確認：
```bash
docker stats
```

2. `.env` でメモリ制限を調整：
```bash
MEMORY_HARD_LIMIT_MB=16384
MEMORY_SOFT_TARGET_MB=8192
```

3. ビルド並列度を調整：
```bash
BUILD_PARALLELISM=4
```

## スケーリング

### 複数の MygramDB インスタンス

複数の MygramDB インスタンスを実行する場合（例：異なるテーブル用）：

```bash
# インスタンスごとに別々の compose ファイルを作成
cp docker-compose.yml docker-compose.instance1.yml
cp docker-compose.yml docker-compose.instance2.yml

# 異なるプロジェクト名とポートを使用
docker-compose -f docker-compose.instance1.yml -p mygramdb1 up -d
docker-compose -f docker-compose.instance2.yml -p mygramdb2 up -d
```

### ロードバランシング

nginx または HAProxy を使用して複数の MygramDB インスタンス間で負荷分散：

```nginx
upstream mygramdb_backend {
    server localhost:11016;
    server localhost:11017;
    server localhost:11018;
}

server {
    listen 80;
    location / {
        proxy_pass http://mygramdb_backend;
    }
}
```

## セキュリティのベストプラクティス

1. **強力なパスワードを使用** - `.env` 内のすべてのデフォルトパスワードを変更
2. **ネットワークの分離** - Docker ネットワークを使用してサービスを分離
3. **localhost にバインド** - 本番環境では、MySQL を `127.0.0.1` のみにバインド
4. **TLS を有効化** - MySQL 接続に TLS を使用
5. **定期的な更新** - Docker イメージを最新に保つ
6. **定期的なバックアップ** - MySQL と MygramDB スナップショットの自動バックアップ

## 参考資料

- [設定ガイド](configuration.md)
- [MySQL レプリケーション設定](mysql-replication.md)
- [パフォーマンスチューニング](performance-tuning.md)
