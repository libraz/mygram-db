# DockerによるLinux CI テスト

## 概要

このガイドでは、GitHub Actions CI と同じLinux環境でコードをテストし、プラットフォーム固有の問題（ヘッダの記載漏れやコンパイラの違い）を**pushする前に**検出する方法を説明します。

## 問題

macOSで開発している場合、以下の問題がLinux環境でのみ発生することがあります：

- **ヘッダファイルの記載漏れ** (例: `<cstdint>`, `<climits>`)
- **コンパイラの挙動の違い** (Clang vs GCC)
- **ライブラリバージョンの違い**
- **パス処理の違い**

これらの問題は多くの場合CIでのみ検出され、pushした後にビルドが失敗します。

## 解決策

GitHub Actions CI環境（Ubuntu 22.04と同じ依存関係）を再現したDockerベースのLinuxテストを提供します。

## クイックスタート

### 1. フルCIチェック（push前に推奨）

```bash
make docker-ci-check
```

これで完全なCIパイプラインを実行します：
1. コードフォーマットチェック
2. ビルド（CIと同じフラグ）
3. clang-tidyリント
4. 全テスト実行

**pushする前に毎回実行**してCIの失敗を未然に防ぎましょう。

### 2. 個別ステップの実行

```bash
# ビルドのみ
make docker-build-linux

# テストのみ
make docker-test-linux

# リントのみ
make docker-lint-linux

# フォーマットチェックのみ
make docker-format-check-linux
```

### 3. 対話的開発シェル

```bash
make docker-dev-shell
```

Linuxコンテナ内で対話的なbashシェルが起動し、手動でコマンドを実行できます：

```bash
# コンテナ内で
make build
make test
make lint
./build/bin/mygramdb --help
```

## 仕組み

### Dockerイメージ: `support/dev/Dockerfile`

`support/dev/Dockerfile` は GitHub Actions CI環境と**完全に一致する**Linux開発環境を作成します：

- **ベース**: Ubuntu 22.04
- **コンパイラ**: GCC（CIと同じ）
- **ツール**: clang-format-18, clang-tidy-18, ccache
- **依存関係**: libmysqlclient-dev, libicu-dev など

### Makefileターゲット

全てのLinuxテストターゲットは `docker-*-linux` パターンに従います：

| ターゲット | 説明 |
|-----------|------|
| `docker-dev-build` | Linux Dockerイメージのビルド（他のターゲットから自動実行） |
| `docker-dev-shell` | Linuxコンテナでの対話的シェル |
| `docker-build-linux` | Linuxでプロジェクトをビルド（CIを再現） |
| `docker-test-linux` | Linuxでテストを実行 |
| `docker-lint-linux` | Linuxでclang-tidyを実行 |
| `docker-format-check-linux` | Linuxでフォーマットをチェック |
| `docker-clean-linux` | Linuxでビルドディレクトリをクリーン |
| `docker-ci-check` | 全CIチェックを実行（推奨） |

## 推奨ワークフロー

### パターン1: push前チェック（推奨）

```bash
# macOSでの通常開発
make format              # コードフォーマット
make build              # ビルド確認
make test               # テスト実行

# push前のLinux確認
make docker-ci-check    # Linux環境で全チェック

# 問題なければコミット
git commit -m "..."
git push
```

### パターン2: 継続的テスト

Linux固有の問題に取り組んでいる場合や頻繁にテストしたい場合：

```bash
# ターミナル1: Linuxでビルドを継続
make docker-build-linux

# ターミナル2: Linuxでテストを継続
make docker-test-linux

# または対話的シェルで高速イテレーション
make docker-dev-shell
# コンテナ内: make build && make test
```

### パターン3: CI失敗時のデバッグ

CIで特定のエラーが発生した場合：

```bash
# CI環境を正確に再現
make docker-dev-shell

# コンテナ内で失敗したステップを実行
cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON -DENABLE_COVERAGE=ON -DUSE_ICU=ON -DUSE_MYSQL=ON ..
make -j$(nproc)
make lint

# 問題を修正後、検証
exit
make docker-ci-check
```

## パフォーマンスのヒント

### 1. イメージキャッシング

Dockerイメージは初回ビルド後にキャッシュされます。以降の実行はずっと高速です：

- **初回**: ~5-10分（LLVMや依存関係のダウンロードとインストール）
- **2回目以降**: ~10-30秒（コードのビルドのみ）

### 2. インクリメンタルビルド

`build/` ディレクトリはボリュームとしてマウントされるため、ビルドはインクリメンタルです：

```bash
# 初回ビルド: 全コンパイル
make docker-build-linux

# 2回目のビルド: 変更ファイルのみ
make docker-build-linux  # ずっと高速！
```

### 3. 並列テスト

テストは利用可能な全CPUコアを使用して並列実行されます：

```bash
make docker-test-linux  # $(nproc) コアを使用
```

### 4. ccache

コンテナはccacheを使用してコンパイルを高速化します。これはビルド間で共有されます。

## トラブルシューティング

### 問題: "permission denied" エラー

コンテナはrootとして実行されますが、ファイルはユーザーが所有しています。権限の問題が発生した場合：

```bash
# ビルドディレクトリをクリーン
make docker-clean-linux

# または手動で
sudo rm -rf build/
```

### 問題: Dockerイメージが古い

依存関係が変更された場合（新しいLLVMバージョン、新しいライブラリなど）：

```bash
# 開発イメージを再ビルド
docker build -f support/dev/Dockerfile -t mygramdb-dev:latest --no-cache .
```

### 問題: ディスク容量

Dockerイメージはディスク容量を消費します。クリーンアップ方法：

```bash
# 開発イメージを削除
docker rmi mygramdb-dev:latest

# 未使用のDockerリソースを全てクリーン
docker system prune -a
```

### 問題: ビルドが遅い

キャッシュを使用してもビルドが遅い場合：

```bash
# ccache統計を確認
make docker-dev-shell
ccache --show-stats

# 必要に応じてccacheサイズを増やす（コンテナ内）
ccache --set-config=max_size=1G
```

## CI環境の詳細

`support/dev/Dockerfile` 環境は `.github/workflows/ci.yml` と一致します：

| コンポーネント | バージョン |
|--------------|----------|
| OS | Ubuntu 22.04 |
| コンパイラ | GCC（デフォルト）+ Clang 18 |
| CMake | aptから最新版 |
| clang-format | 18 |
| clang-tidy | 18 |
| MySQL Client | libmysqlclient-dev |
| ICU | libicu-dev |
| Readline | libreadline-dev |
| カバレッジ | lcov |
| キャッシュ | ccache |

## Git Hooksとの連携

push前に自動的にCIチェックを実行するpre-pushフックを追加できます：

```bash
# .git/hooks/pre-push
#!/bin/bash
echo "Running CI checks before push..."
make docker-ci-check
if [ $? -ne 0 ]; then
    echo "CI checks failed. Push aborted."
    exit 1
fi
```

```bash
chmod +x .git/hooks/pre-push
```

## 高度な使用方法

### カスタムCMakeオプション

```bash
# 異なるオプションでビルド
docker run --rm -v $(pwd):/workspace -w /workspace mygramdb-dev:latest \
    bash -c "mkdir -p build && cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release -DENABLE_ASAN=ON .. && \
    make -j$(nproc)"
```

### 特定のテストを実行

```bash
docker run --rm -v $(pwd):/workspace -w /workspace mygramdb-dev:latest \
    bash -c "cd build && ctest -R 'MyTest.*' --verbose"
```

### カバレッジレポートの生成

```bash
make docker-dev-shell
# コンテナ内
cd build
make coverage
# coverage/html/index.htmlを表示
```

## 比較: macOSとLinuxのテスト

| 側面 | macOS (`make build`) | Linux (`make docker-build-linux`) |
|------|----------------------|----------------------------------|
| 速度 | 高速（ネイティブ） | やや遅い（コンテナ化） |
| 環境 | あなたのmacOS | CI環境（Ubuntu 22.04） |
| 用途 | 高速イテレーション | push前の検証 |
| コンパイラ | Clang（macOS） | GCC（Linux） |
| ヘッダ | より寛容 | より厳格（記載漏れを検出） |

**推奨**: 両方を使用！
- macOS: 迅速な開発用
- Linux: push前の検証用

## 関連ドキュメント

- [GitHub Actions CIワークフロー](../../.github/workflows/ci.yml)
- [Docker Composeセットアップ](../../docker-compose.yml)
- [開発ガイド](./development.md)
