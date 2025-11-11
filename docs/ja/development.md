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
brew install llvm  # clang、clang-tidy、clang-format、clangd を含む
brew install mysql-client@8.4
brew install icu4c

# LLVM を PATH に追加（オプション、最新の clangd を使用する場合）
echo 'export PATH="/opt/homebrew/opt/llvm/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Linux（Ubuntu/Debian）

```bash
# パッケージリストを更新
sudo apt-get update

# 開発ツールをインストール
sudo apt-get install -y cmake
sudo apt-get install -y clang clang-tidy clang-format clangd
sudo apt-get install -y libmysqlclient-dev
sudo apt-get install -y libicu-dev
sudo apt-get install -y pkg-config
```

## インストールの確認

```bash
# ツールがインストールされているか確認
cmake --version        # 3.15+ であることを確認
clang++ --version      # C++17 サポートを確認
clang-tidy --version   # リンティング用
clang-format --version # フォーマット用
clangd --version       # LSP 用（オプション）
```

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
- カラム制限: 100 文字
- インデント: 2 スペース（タブなし）
- ポインタ配置: 左（`int* ptr`、`int *ptr` ではない）
- ブレース: アタッチスタイル（`if (x) {`）

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

**解決策:** clang-tidy をインストール：

```bash
# macOS
brew install llvm

# Linux
sudo apt-get install clang-tidy
```

### "Many errors in VS Code after opening project"

**解決策:**
1. プロジェクトがビルドされていることを確認: `make`
2. `build/compile_commands.json` が存在することを確認
3. VS Code ウィンドウをリロード: Cmd/Ctrl + Shift + P → "Developer: Reload Window"

### "clang-format not working"

**解決策:**

```bash
# macOS
brew install clang-format

# Linux
sudo apt-get install clang-format
```

`.vscode/settings.json` のパスがインストールと一致することを確認：

```bash
which clang-format  # 実際のパスを確認
```

## クイックスタートチェックリスト

- [ ] cmake、clang、clang-tidy、clang-format をインストール
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

## コントリビューション

変更を送信する前に：

- [ ] コードが Google C++ スタイルガイドに従っている
- [ ] すべてのコメントとドキュメントが英語である
- [ ] 公開 API に Doxygen コメントを追加
- [ ] ユニットテストを追加/更新
- [ ] すべてのテストが成功（`make test`）
- [ ] コードが clang-format でフォーマットされている（`make format`）
- [ ] コンパイラ警告がない
- [ ] ドキュメントを更新（必要な場合）
