# MygramDB

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mygram-db/ci.yml?branch=main&label=CI)](https://github.com/libraz/mygram-db/actions)
[![Version](https://img.shields.io/github/v/release/libraz/mygram-db?label=version)](https://github.com/libraz/mygram-db/releases)
[![codecov](https://codecov.io/gh/libraz/mygram-db/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mygram-db)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libraz/mygram-db/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mygram-db)
[![Docs](https://img.shields.io/badge/docs-mygramdb.libraz.net-2563eb)](https://mygramdb.libraz.net/ja/)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue?logo=docker)](https://github.com/libraz/mygram-db/pkgs/container/mygram-db)
[![MySQL](https://img.shields.io/badge/MySQL-8.4--9.6-blue?logo=mysql)](https://dev.mysql.com/)

**MygramDB は、MySQL/MariaDB の binlog から同期するインメモリ全文検索エンジンです。** MySQL/MariaDB を正本のままにして、検索トラフィックだけを MygramDB に送れます。

**こんなときに使えます**

- **MySQL のデータを速く検索したい** — n-gram 検索、filter、facet、BM25 ソート、highlight、fuzzy search をメモリ上で実行します。
- **書き込み経路を変えたくない** — GTID binlog replication が INSERT / UPDATE / DELETE と対応する schema change を取り込みます。
- **検索専用レプリカを運用したい** — 読み取りは TCP または HTTP、書き込みと SQL は引き続き MySQL/MariaDB が担当します。

📖 **[ドキュメント](https://mygramdb.libraz.net/ja/)** &nbsp;·&nbsp; **[はじめに](https://mygramdb.libraz.net/ja/docs/getting-started)** &nbsp;·&nbsp; **[Docker デプロイ](https://mygramdb.libraz.net/ja/docs/docker-deployment)** &nbsp;·&nbsp; **[クエリ構文](https://mygramdb.libraz.net/ja/docs/query-syntax)** &nbsp;·&nbsp; **[運用](https://mygramdb.libraz.net/ja/docs/operations)**

## 含まれるもの

- GTID ベースの MySQL 8.4+/9.x、MariaDB 10.6+/11.x レプリケーション
- TCP プロトコル、HTTP API、C/C++ クライアントライブラリ
- 複数テーブルの index、runtime configuration、DUMP save/load、MySQL failover
- CJK を含む多言語テキストの ICU 正規化

インストール、設定、デプロイ、プロトコルと HTTP API、レプリケーション要件、性能データは [ドキュメント](https://mygramdb.libraz.net/ja/) にあります。

## 開発

```bash
make build
make test
```

完全な test matrix、E2E、sanitizer、ローカル fuzz は [開発ガイド](https://mygramdb.libraz.net/ja/docs/development) を参照してください。

## ライセンス

[MIT License](LICENSE)

## 関連プロジェクト

- [mysql-event-stream](https://github.com/libraz/mysql-event-stream) — MygramDB から切り出した MySQL/MariaDB CDC ライブラリ
- [go-mygram-client](https://github.com/libraz/go-mygram-client) — Go クライアントライブラリ
- [node-mygramdb-client](https://github.com/libraz/node-mygramdb-client) — Node.js クライアントライブラリ
- [python-mygramdb-client](https://github.com/libraz/python-mygramdb-client) — Python クライアントライブラリ
