# インストールガイド

このガイドでは、MygramDB のビルドとインストールの詳細な手順を説明します。

## 前提条件

- C++17 対応コンパイラ（GCC 7+、Clang 5+）
- CMake 3.15+
- MySQL クライアントライブラリ（libmysqlclient）
- ICU ライブラリ（libicu）

### 依存関係のインストール

#### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y pkg-config libmysqlclient-dev libicu-dev cmake g++
```

#### macOS

```bash
brew install cmake mysql-client@8.4 icu4c pkg-config
```

## ソースからのビルド

### Makefile を使用（推奨）

```bash
# リポジトリのクローン
git clone https://github.com/libraz/mygram-db.git
cd mygram-db

# ビルド
make

# テスト実行
make test

# ビルドをクリーン
make clean

# その他の便利なコマンド
make help      # 利用可能なコマンド一覧を表示
make rebuild   # クリーン後に再ビルド
make format    # clang-format でコード整形
```

### CMake を直接使用

```bash
# ビルドディレクトリの作成
mkdir build && cd build

# 設定とビルド
cmake ..
cmake --build .

# テスト実行
ctest
```

## バイナリのインストール

### システム全体へのインストール

`/usr/local` にインストール（デフォルト）：

```bash
sudo make install
```

以下がインストールされます：
- バイナリ: `/usr/local/bin/mygramdb`, `/usr/local/bin/mygram-cli`
- 設定サンプル: `/usr/local/etc/mygramdb/config.yaml.example`
- ドキュメント: `/usr/local/share/doc/mygramdb/`

### カスタムディレクトリへのインストール

任意のディレクトリにインストール：

```bash
make PREFIX=/opt/mygramdb install
```

### アンインストール

インストールしたファイルを削除：

```bash
sudo make uninstall
```

## テストの実行

Makefile を使用:

```bash
make test
```

または CTest を直接使用:

```bash
cd build
ctest --output-on-failure
```

現在のテストカバレッジ: **169 テスト、100% 成功**

### 統合テスト

すべてのユニットテストは MySQL サーバー接続なしで実行できます。MySQL サーバーが必要な統合テストは分離されており、デフォルトでは無効化されています。

統合テストを実行するには：

```bash
# MySQL 接続用の環境変数を設定
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=your_password
export MYSQL_DATABASE=test
export ENABLE_MYSQL_INTEGRATION_TESTS=1

# 統合テストを実行
./build/bin/mysql_connection_integration_test
```

## ビルドオプション

Makefile を使用する際に CMake オプションを設定できます:

```bash
# AddressSanitizer を有効化
make CMAKE_OPTIONS="-DENABLE_ASAN=ON" configure

# ThreadSanitizer を有効化
make CMAKE_OPTIONS="-DENABLE_TSAN=ON" configure

# テストを無効化
make CMAKE_OPTIONS="-DBUILD_TESTS=OFF" configure
```

## インストールの確認

インストール後、バイナリが利用可能かを確認：

```bash
# サーバーバイナリを確認
mygramdb --help

# CLI クライアントを確認
mygram-cli --help
```

## サービスとして実行（systemd）

MygramDB は**セキュリティ上の理由からrootでの実行を拒否**します。非特権ユーザーで実行する必要があります。

### 1. 専用ユーザーの作成

```bash
sudo useradd -r -s /bin/false mygramdb
```

### 2. 必要なディレクトリの作成

```bash
sudo mkdir -p /etc/mygramdb /var/lib/mygramdb/dumps
sudo chown -R mygramdb:mygramdb /var/lib/mygramdb
```

### 3. 設定ファイルのコピー

```bash
sudo cp examples/config.yaml /etc/mygramdb/config.yaml
sudo chown mygramdb:mygramdb /etc/mygramdb/config.yaml
sudo chmod 600 /etc/mygramdb/config.yaml  # 認証情報を保護
```

### 4. systemd サービスのインストール

```bash
sudo cp support/systemd/mygramdb.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 5. サービスの起動と有効化

```bash
# サービスの起動
sudo systemctl start mygramdb

# ステータス確認
sudo systemctl status mygramdb

# ブート時の自動起動を有効化
sudo systemctl enable mygramdb

# ログの確認
sudo journalctl -u mygramdb -f
```

## 手動実行（デーモンモード）

手動操作や従来型のinitシステムの場合、`-d` / `--daemon` オプションを使用できます：

```bash
# デーモンとして実行（バックグラウンドプロセス）
sudo -u mygramdb mygramdb -d -c /etc/mygramdb/config.yaml

# 実行確認
ps aux | grep mygramdb

# 停止（SIGTERM送信）
pkill -TERM mygramdb
```

**注意**: デーモンとして実行する場合、すべての出力は `/dev/null` にリダイレクトされます。必要に応じて設定でファイルベースのログを設定してください。

## セキュリティに関する注意事項

- **root実行のブロック**: MygramDB は root として起動すると拒否されます
- **推奨方法**: systemd の `User=` および `Group=` ディレクティブを使用（`support/systemd/mygramdb.service` を参照）
- **Docker**: 既に非root ユーザー `mygramdb` として実行するよう設定済み
- **ファイルパーミッション**: 設定ファイルは mygramdb ユーザーのみが読み取り可能にすべき（モード 600）
- **デーモンモード**: 従来型initシステムや手動バックグラウンド実行には `-d` / `--daemon` を使用

## 次のステップ

インストールが成功したら：

1. [設定ガイド](configuration.md) を参照して設定ファイルをセットアップ
2. [レプリケーションガイド](replication.md) を参照して MySQL レプリケーションを設定
3. `mygramdb -c config.yaml` を非rootユーザーまたはsystemd経由で実行
