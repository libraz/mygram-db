# 開発環境セットアップ

このガイドでは、リアルタイムリンティングと自動フォーマット（JavaScript の ESLint/Prettier のような機能）を備えた MygramDB の開発環境をセットアップする方法を説明します。

## 前提条件

開発を始める前に、必要なツールをインストールします。

### macOS

```bash
# Homebrew をインストール（まだインストールしていない場合）
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 開発ツールをインストール
brew install cmake
brew install llvm@18  # バージョン 18 が必要（一貫したフォーマットのため）
brew install mysql-client@8.4
brew install icu4c

# LLVM 18 をデフォルトで使用するためのシンボリックリンクを作成
# 重要: CI と一貫したコードフォーマットを保証するため
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-format /opt/homebrew/bin/clang-format
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-tidy /opt/homebrew/bin/clang-tidy
ln -sf /opt/homebrew/opt/llvm@18/bin/clangd /opt/homebrew/bin/clangd
```

### Linux（Ubuntu/Debian）

```bash
# パッケージリストを更新
sudo apt-get update

# 基本的な開発ツールをインストール
sudo apt-get install -y \
  cmake \
  build-essential \
  libmysqlclient-dev \
  libicu-dev \
  pkg-config \
  wget \
  lsb-release \
  software-properties-common \
  gnupg

# LLVM/Clang 18 をインストール（一貫したフォーマットのため必須）
# 重要: CI 環境と一致させるためバージョン 18 が必要
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18

# clang-format、clang-tidy、clangd バージョン 18 をインストール
sudo apt-get install -y clang-format-18 clang-tidy-18 clangd-18

# バージョン 18 をデフォルトに設定
sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-18 100
sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-18 100
sudo update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-18 100
```

## インストールの確認

```bash
# ツールがインストールされているか確認
cmake --version        # 3.15+ であることを確認
clang-format --version # 18.x.x であることを確認
clang-tidy --version   # 18.x.x であることを確認
clangd --version       # 18.x.x であることを確認（オプション、LSP 用）
```

**なぜバージョン 18 が必要なのか？** clang-format のバージョンが異なると、フォーマット結果も異なります。ローカル開発環境と CI の一貫性を保つため、バージョン 18 に統一しています。

## プロジェクトのビルド

```bash
# プロジェクトをビルド（compile_commands.json を生成）
make

# テストを実行
make test

# コードをフォーマット
make format
```

ビルド後、`compile_commands.json` が `build/` ディレクトリに生成されます。このファイルは clangd と clang-tidy が正しく動作するために必要です。

## VS Code セットアップ

### 必須エクステンション

VS Code を開いて、推奨エクステンションをインストールします：

1. **C/C++**（`ms-vscode.cpptools`）- IntelliSense とデバッグ
2. **CMake Tools**（`ms-vscode.cmake-tools`）- CMake 統合
3. **Error Lens**（`usernamehw.errorlens`）- インラインエラー表示（ESLint のような）

### オプションのエクステンション（最高の体験のため）

4. **clangd**（`llvm-vs-code-extensions.vscode-clangd`）- リアルタイムリンティング用の高速 LSP

## リンティングを有効化

プロジェクトをビルドした後、VS Code でリンティングを有効化します：

### オプション A: C/C++ エクステンションを使用（シンプル）

`.vscode/settings.json` を編集して変更：

```json
"C_Cpp.codeAnalysis.clangTidy.enabled": true,
"C_Cpp.codeAnalysis.clangTidy.useBuildPath": true,
```

VS Code ウィンドウをリロード（Cmd/Ctrl + Shift + P → "Developer: Reload Window"）

### オプション B: clangd を使用（高速、ESLint のような体験）

1. clangd エクステンションがインストールされていることを確認
2. `.vscode/settings.json` を編集して clangd セクションをアンコメント（OPTION 1）
3. C/C++ エクステンションセクションをコメントアウト（OPTION 2）
4. VS Code ウィンドウをリロード

## 有効化される機能

セットアップ後、以下が利用可能になります：

- ✅ **リアルタイムリンティング** - タイプしながら clang-tidy がチェック
- ✅ **自動フォーマット** - 保存時にコードをフォーマット（Google C++ スタイル）
- ✅ **インラインエラー** - Error Lens でインラインにエラー表示
- ✅ **コード補完** - C++17 用の IntelliSense
- ✅ **クイックフィックス** - 保存時に利用可能な問題を自動修正

## コーディング規約

このプロジェクトは特定の設定で **Google C++ スタイルガイド** に従います：

### コードフォーマット

- ベーススタイル: Google
- カラム制限: 120 文字
- インデント: 2 スペース（タブなし）
- ポインタ配置: 左（`int* ptr`、`int *ptr` ではない）
- ブレース: アタッチスタイル（`if (x) {`）

フォーマットツールの実行:

```bash
make format        # コードを自動整形
make format-check  # フォーマットをチェック（CI モード）
make lint          # clang-tidy で静的解析（時間がかかる）
```

### 命名規則

| 要素 | 規則 | 例 |
|------|------|-----|
| クラス | CamelCase | `DocumentStore`, `Index` |
| 関数 | CamelCase | `AddDocument()`, `GetPrimaryKey()` |
| 変数 | lower_case | `doc_id`, `primary_key` |
| 定数 | kCamelCase | `kMaxConnections`, `kDefaultPort` |
| メンバー変数 | lower_case_ | `next_doc_id_`, `term_postings_` |
| 名前空間 | lower_case | `mygramdb::index` |

### ドキュメントコメント

すべての公開 API に Doxygen スタイルのコメントを使用：

```cpp
/**
 * @brief 関数が行うことの簡単な説明
 *
 * @param param_name パラメータの説明
 * @return 戻り値の説明
 */
ReturnType FunctionName(Type param_name);
```

### clang-tidy 警告の抑制

必要に応じて、NOLINTコメントで警告を抑制します:

```cpp
// ✅ 良い例: 問題のある行の前に NOLINTNEXTLINE を使用
// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
snprintf(buf, sizeof(buf), "%d", value);

// ✅ 良い例: インライン NOLINT（120文字以内に収める）
char buf[32];  // NOLINT(cppcoreguidelines-avoid-c-arrays)

// ❌ 悪い例: 複数行の NOLINT（clang-tidy が認識しない）
snprintf(buf, sizeof(buf), "%d",
         value);  // NOLINT(cppcoreguidelines-pro-type-vararg)

// ✅ 良い例: ファイルレベルの抑制（広範囲に影響する問題）
// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
// ... ポインタ演算が必要なコード ...
// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
```

**重要:** NOLINTコメントは1行で記述する必要があります。複数行のコメントは認識されません。

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

## トラブルシューティング

### "clangd: Unable to find compile_commands.json"

**解決策:** 最初にプロジェクトをビルドするため `make` を実行。

### "clang-tidy: error while loading shared libraries"

**解決策:** clang-tidy バージョン 18 をインストール：

```bash
# macOS
brew install llvm@18
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-tidy /opt/homebrew/bin/clang-tidy

# Linux
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18
sudo apt-get install -y clang-tidy-18
sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-18 100
```

### "Many errors in VS Code after opening project"

**解決策:**
1. プロジェクトがビルドされていることを確認: `make`
2. `build/compile_commands.json` が存在することを確認
3. VS Code ウィンドウをリロード: Cmd/Ctrl + Shift + P → "Developer: Reload Window"

### "clang-format not working"

**解決策:** clang-format バージョン 18 をインストール：

```bash
# macOS
brew install llvm@18
ln -sf /opt/homebrew/opt/llvm@18/bin/clang-format /opt/homebrew/bin/clang-format

# Linux
wget https://apt.llvm.org/llvm.sh
chmod +x llvm.sh
sudo ./llvm.sh 18
sudo apt-get install -y clang-format-18
sudo update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-18 100
```

バージョンを確認：

```bash
clang-format --version  # 18.x.x であることを確認
```

## クイックスタートチェックリスト

- [ ] cmake をインストール
- [ ] LLVM 18（clang-format-18、clang-tidy-18、clangd-18）をインストール
- [ ] clang-format --version で 18.x.x が表示されることを確認
- [ ] MySQL クライアントライブラリをインストール
- [ ] ICU ライブラリをインストール
- [ ] `make` を実行してプロジェクトをビルド
- [ ] `build/compile_commands.json` が存在することを確認
- [ ] VS Code エクステンションをインストール（C/C++、CMake Tools、Error Lens）
- [ ] `.vscode/settings.json` で clang-tidy を有効化
- [ ] VS Code ウィンドウをリロード

## 次のステップ

セットアップが完了したら：

1. 任意の `.cpp` または `.h` ファイルを開く
2. 小さな変更を加える（例: スペースを追加）
3. ファイルを保存 → 自動フォーマットが動作するはず
4. スタイルガイドに違反するコードを追加してみる → 警告が表示されるはず

## 開発の優先順位

機能を実装したり変更を加えたりする場合、以下の順序で優先順位を付けます：

1. **パフォーマンス**: 速度と低レイテンシを最適化
2. **メモリ効率**: メモリフットプリントを最小化
3. **保守性**: クリーンでテスト可能なコードを書く

詳細なコーディングガイドラインについては、プロジェクトルートの [CLAUDE.md](../../CLAUDE.md) を参照してください。

## Linux CI テスト（macOS開発者向け）

macOSで開発している場合、一部の問題（ヘッダの記載漏れ、コンパイラの違いなど）はLinux環境でのみ発生し、CIで失敗します。

**解決策**: **pushする前に**GitHub Actions CI と同じLinux環境でコードをテストします。

```bash
# フルCIチェック（git pushの前に推奨）
make docker-ci-check
```

これで以下を実行します：
1. コードフォーマットチェック
2. ビルド（CIと同じフラグ）
3. clang-tidyリント
4. 全テスト実行

**個別チェック:**

```bash
make docker-build-linux       # ビルドのみ
make docker-test-linux        # テストのみ
make docker-lint-linux        # リントのみ
make docker-format-check-linux # フォーマットチェックのみ
```

**対話的シェル:**

```bash
make docker-dev-shell         # デバッグ用にLinuxコンテナに入る
```

詳細は [Linux CIテストガイド](./linux-testing.md) を参照してください。

## 私の開発ワークフロー（参考情報）

これは私がコード品質を維持するために行っている個人的なワークフローです。**必ずしもこれに従う必要はありません** - 重要なのはPRがCIチェックを通過することです。

### コミット前に通常行っていること

1. **コードを記述** - Google C++ Style Guide に従う
2. **`make format` を実行** - コードを自動整形
3. **`make lint` を実行** - 問題を早期発見（遅いですが価値あり）
4. **`make test` を実行** - すべてのテストが通ることを確認
5. **`make docker-ci-check` を実行** - Linux互換性を検証（macOSのみ）

### CI要件

以下はCIで自動的にチェックされます:

- ✅ コードフォーマット（clang-format）
- ✅ 静的解析（clang-tidy）
- ✅ すべてのテストが通る
- ✅ コンパイラ警告なし
- ✅ Linuxビルド互換性

**お好みでCIに任せることもできます。** PRがすべてのCIチェックを通過していればOKです。
