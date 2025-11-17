# Development Support Tools

このディレクトリには開発用のツールとスクリプトが含まれています。

## ファイル一覧

### Docker Linux Testing

macOS開発者がLinux環境（CI環境）でのビルド問題を事前検知するためのツール。

- **Dockerfile** - CI環境（Ubuntu 22.04）を再現する開発用Dockerイメージ

**使い方:**
```bash
# コミット前の全チェック（推奨）
make docker-ci-check

# 対話的シェル
make docker-dev-shell

# 個別チェック
make docker-build-linux
make docker-test-linux
make docker-lint-linux
```

**詳細:** [Linux Testing Guide](../../docs/ja/linux-testing.md) (日本語) / [English](../../docs/en/linux-testing.md)

### Code Quality Tools

- **run-clang-tidy.sh** - clang-tidyを実行するスクリプト

**使い方:**
```bash
make lint
```

## 関連ドキュメント

- [Linux Testing Guide](../../docs/ja/linux-testing.md) - Linux CIテストガイド（日本語）
- [Linux Testing Guide (EN)](../../docs/en/linux-testing.md) - Linux CI Testing Guide (English)
- [Development Guide](../../docs/ja/development.md) - 開発環境セットアップガイド（日本語）
- [Development Guide (EN)](../../docs/en/development.md) - Development Guide (English)
- [CLAUDE.md](../../CLAUDE.md) - 開発ガイドライン
