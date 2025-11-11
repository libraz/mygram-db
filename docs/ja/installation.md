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

## 次のステップ

インストールが成功したら：

1. [設定ガイド](configuration.md) を参照して設定ファイルをセットアップ
2. [レプリケーションガイド](replication.md) を参照して MySQL レプリケーションを設定
3. `mygramdb config.yaml` を実行してサーバーを起動
