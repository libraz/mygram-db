# クエリ構文ガイド

MygramDBは、複雑なテキスト検索操作のための豊富なブール演算クエリ構文をサポートしています。

## 目次

- [基本構文](#基本構文)
- [ブール演算子](#ブール演算子)
- [複雑なブール演算クエリ](#複雑なブール演算クエリ)
- [演算子の優先順位](#演算子の優先順位)
- [引用符によるフレーズ検索](#引用符によるフレーズ検索)
- [フィルタ条件](#フィルタ条件)
- [ソート (SORT句)](#ソート-sort句)
- [ページネーション (LIMIT/OFFSET)](#ページネーション-limitoffset)
- [エラーハンドリング](#エラーハンドリング)
- [パフォーマンスチップス](#パフォーマンスチップス)

---

## 基本構文

### コマンドフォーマット

```
SEARCH <table> <query_expression> [FILTER ...] [SORT ...] [LIMIT ...] [OFFSET ...]
COUNT <table> <query_expression> [FILTER ...]
```

### シンプルな検索

```
SEARCH <table> <term>
```

例：
```
SEARCH threads golang
```

### レスポンスフォーマット

**SEARCHレスポンス:**
```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

例：
```
OK RESULTS 3 101 205 387
```

**COUNTレスポンス:**
```
OK COUNT <number>
```

例：
```
OK COUNT 42
```

---

## ブール演算子

### AND検索

**すべて**の指定された用語を含むドキュメントを検索します。

```
SEARCH <table> term1 AND term2 AND term3
```

例：
```
SEARCH threads golang AND tutorial
```

### OR検索

指定された用語の**いずれか**を含むドキュメントを検索します。

```
SEARCH <table> term1 OR term2 OR term3
```

例：
```
SEARCH threads golang OR python OR rust
```

### NOT検索

特定の用語を含むドキュメントを除外します。

```
SEARCH <table> term1 NOT term2
```

例：
```
SEARCH threads tutorial NOT beginner
```

**重要:** NOTは結果セットからドキュメントを除外します。大規模インデックスでは慎重に使用してください。

---

## 複雑なブール演算クエリ

### 優先順位を制御する括弧

括弧を使用して式をグループ化し、演算子の優先順位を制御できます：

```
SEARCH <table> (term1 OR term2) AND term3
```

例：
```
SEARCH threads (golang OR python) AND tutorial
```

これは「tutorial」**AND**（「golang」**OR**「python」）を含むドキュメントを検索します。

### ネストした式

複数レベルの括弧をネストできます：

```
SEARCH <table> ((term1 OR term2) AND term3) OR term4
```

例：
```
SEARCH threads ((golang OR python) AND web) OR rust
```

これは以下を検索します：
- 「web」**AND**（「golang」**OR**「python」）を含むドキュメント
- **または**「rust」を含むドキュメント

### 複雑なクエリの例

#### GoまたはPythonのチュートリアルを検索し、初心者向けコンテンツを除外

```
SEARCH threads (golang OR python) AND tutorial NOT beginner
```

#### MySQLまたはPostgreSQLに関するデータベースコンテンツを検索し、SQLiteを除外

```
SEARCH posts database AND (mysql OR postgresql) NOT sqlite
```

#### PythonまたはRで機械学習コンテンツを検索し、TensorFlowを除外

```
SEARCH articles "machine learning" AND (python OR R) NOT tensorflow
```

---

## 演算子の優先順位

括弧が使用されていない場合、演算子は以下の優先順位を持ちます（高い順）：

1. **NOT** （最高）
2. **AND** （中）
3. **OR** （最低）

### 優先順位の例

**クエリ:** `a OR b AND c`
**解釈:** `a OR (b AND c)`

**クエリ:** `NOT a AND b`
**解釈:** `(NOT a) AND b`

**クエリ:** `a AND b OR c AND d`
**解釈:** `(a AND b) OR (c AND d)`

**ベストプラクティス:** 厳密に必要でない場合でも、括弧を使用して意図を明確にしてください。

---

## 引用符によるフレーズ検索

完全一致のフレーズ検索には、ダブルクォーテーション `"` またはシングルクォーテーション `'` を使用します：

```
SEARCH <table> "exact phrase"
SEARCH <table> 'machine learning'
```

### エスケープシーケンス

引用符で囲まれた文字列内でサポートされているエスケープシーケンス：

- `\n` - 改行
- `\t` - タブ
- `\r` - キャリッジリターン
- `\\` - バックスラッシュ
- `\"` - ダブルクォーテーション
- `\'` - シングルクォーテーション

例：
```
SEARCH articles "hello \"world\""
```

### 引用符とブール演算子の組み合わせ

引用符で囲まれたフレーズはブール演算子と組み合わせることができます：

```
SEARCH threads "web framework" AND (golang OR python)
SEARCH posts "machine learning" NOT "deep learning"
```

---

## フィルタ条件

`FILTER`句を使用して、カラム値で結果をフィルタリングします。

### 構文

```
SEARCH <table> <query> FILTER <column> <operator> <value> [FILTER <col> <op> <val> ...]
```

複数のフィルタを指定できます（すべて一致する必要があります - AND論理）。

### サポートされる演算子

- `=` または `EQ` - 等しい
- `!=` または `NE` - 等しくない
- `>` または `GT` - より大きい
- `>=` または `GTE` - 以上
- `<` または `LT` - より小さい
- `<=` または `LTE` - 以下

### 例

**単一フィルタ:**
```
SEARCH articles tech FILTER status = 1
```

**複数フィルタ:**
```
SEARCH articles tech FILTER status = 1 FILTER category = ai
```

**比較演算子:**
```
SEARCH articles tech FILTER views > 1000
SEARCH articles tech FILTER created_at >= 2024-01-01
SEARCH articles tech FILTER priority != 0
```

**ブール演算クエリと組み合わせ:**
```
SEARCH threads (golang OR python) AND tutorial FILTER status = published
```

### フィルタカラムの型

MygramDBは、インデックス化されたフィルタカラムでのフィルタリングをサポートしています：

- **整数**: `status=1`, `priority=5`
- **文字列**: `category=tech`, `author=john`
- **日付/時刻**: `created_at=2024-01-15T10:30:00`

**注意:** `config.yaml`でフィルタとして設定されたカラムのみがFILTER句で使用できます。

### フィルタのパフォーマンス

- **ビットマップインデックス**: 低カーディナリティカラム（例：status, category）で非常に高速
- **辞書圧縮**: 文字列カラムで効率的
- **フィルタリング順序**: フィルタはテキスト検索の積集合の後に適用されます

---

## ソート (SORT句)

`SORT`句を使用して検索結果をソートします。

### 構文

```
SEARCH <table> <query> SORT <column> [ASC|DESC]
```

**注意:** `ORDER BY`構文はサポートされていません。代わりに`SORT`を使用してください。

### デフォルトの動作

`SORT`が指定されていない場合、結果は**プライマリキーの降順**でソートされます（最新が最初）。

```
SEARCH threads golang
-- 以下と同等: SEARCH threads golang SORT id DESC
```

### プライマリキーでソート

**完全な構文:**
```
SEARCH threads golang SORT id ASC
SEARCH threads golang SORT id DESC
```

**省略記法（推奨）:**
```
SEARCH threads golang SORT ASC   -- プライマリキー昇順
SEARCH threads golang SORT DESC  -- プライマリキー降順
```

### フィルタカラムでソート

インデックス化された任意のフィルタカラムでソート：

```
SEARCH threads golang SORT created_at DESC LIMIT 10
SEARCH posts database SORT score ASC LIMIT 20
```

### ブール演算クエリとの組み合わせ

```
SEARCH threads (golang OR python) AND tutorial SORT created_at DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND database) NOT sqlite SORT score ASC
```

### パフォーマンスの考慮事項

**ソートアルゴリズム:**
- **LIMITあり**: `partial_sort`を使用 - O(N × log(K)) ただし K = LIMIT + OFFSET
- **LIMITなし**: 完全ソート - O(N × log(N))
- **メモリ**: インプレースソート、追加メモリ割り当てなし

**大規模な結果セット（例：100万件中80万件がヒット）の場合:**
- partial_sort最適化を活用するため、可能な限り**常にLIMITを使用**してください（約3倍高速）
- 数値キーの場合、プライマリキーのソートはフィルタカラムより高速です
- 結果はOFFSET/LIMIT適用**前に**ソートされます（正しいページネーション）

**パフォーマンス例:**
- 80万件の結果でLIMIT 100の場合：partial_sortで約3倍高速
- インプレースソート：メモリオーバーヘッドなし

### カラム検証

- **プライマリキー**: 常に有効
- **フィルタカラム**: 少なくとも1つのドキュメントに存在する必要があります
- **存在しないカラム**: 警告としてログに記録され、NULL値として扱われます（最後にソート）

---

## ページネーション (LIMIT/OFFSET)

`LIMIT`と`OFFSET`を使用して返される結果の数を制御します。

### LIMIT - 最大結果数

```
SEARCH <table> <query> LIMIT <n>
```

例：
```
SEARCH articles tech LIMIT 10
```

**デフォルト:** 100（config.yamlの`api.default_limit`で設定可能）
**範囲:** 5-1000（`kMinLimit`と`kMaxLimit`で設定可能）

### OFFSET - 結果のスキップ

```
SEARCH <table> <query> OFFSET <n>
```

例：
```
SEARCH articles tech LIMIT 10 OFFSET 20
```

これは結果21-30を返します（最初の20件をスキップ）。

### ページネーションの例

**ページ1（最初の10件）:**
```
SEARCH articles tech LIMIT 10 OFFSET 0
```

**ページ2（結果11-20）:**
```
SEARCH articles tech LIMIT 10 OFFSET 10
```

**ページ3（結果21-30）:**
```
SEARCH articles tech LIMIT 10 OFFSET 20
```

### クエリ長の上限

MygramDB は、検索語・AND/NOT 条件・FILTER 値を合計したクエリ式が設定された長さを超えると `ERROR` を返します。

- **デフォルト:** 128文字
- **設定:** `api.max_query_length`（`0` で無効化）
- **エラー例:** `ERROR Query expression length (...) exceeds ...`

複雑な条件が必要な場合は、`config.yaml` で上限を調整するか、複数のクエリに分割してください。

### すべてのオプションを含む完全な例

```
SEARCH threads (golang OR python) AND tutorial
  FILTER status = published
  SORT created_at DESC
  LIMIT 10
  OFFSET 20
```

このクエリは：
1. 「tutorial」**AND**（「golang」**OR**「python」）を含むドキュメントを検索
2. 公開されたドキュメントのみにフィルタリング
3. 作成日でソート（最新が最初）
4. 結果21-30を返す（ページ3、1ページ10件）

### ページネーションのパフォーマンス

- **LIMIT最適化**: partial_sortを有効化（大規模結果セットで大幅に高速）
- **OFFSETコスト**: O(N) ただし N = OFFSET（結果は生成されますが返されません）
- **ベストプラクティス**: 一貫性のあるページネーションのため、SORTと共にLIMITを使用
- **深いページネーション**: 大きなOFFSET値（例：10000+）は遅くなる可能性があります

---

## エラーハンドリング

### 無効なクエリ

以下のクエリはエラーを返します：

**空の括弧:**
```
SEARCH threads ()
ERROR Invalid query: empty expression in parentheses
```

**閉じられていない括弧:**
```
SEARCH threads (golang AND python
ERROR Invalid query: unclosed parentheses
```

**余分な閉じ括弧:**
```
SEARCH threads golang AND python)
ERROR Invalid query: unexpected closing parenthesis
```

**オペランドのない演算子:**
```
SEARCH threads AND
ERROR Invalid query: operator without operands
```

**末尾の演算子:**
```
SEARCH threads golang AND
ERROR Invalid query: trailing operator
```

**閉じられていない引用符:**
```
SEARCH threads "golang tutorial
ERROR Invalid query: unclosed quote
```

### 無効なフィルタ

**存在しないテーブル:**
```
SEARCH nonexistent tech
ERROR Table not found: nonexistent
```

**無効なフィルタカラム:**
```
SEARCH articles tech FILTER invalid_column=1
ERROR Filter column not found: invalid_column
```

### 無効なソート

**存在しないカラム:**
```
SEARCH articles tech SORT nonexistent DESC
WARNING Column 'nonexistent' not found in documents, treating as NULL
```

注意：存在しないカラムは警告を生成しますがエラーにはなりません（NULLとして扱われます）。

---

## パフォーマンスチップス

### 1. 制約の強い用語を先に配置

```
-- 良い: 具体的な用語を先に
SEARCH articles "machine learning" AND tutorial

-- 最適ではない: 一般的な用語を先に
SEARCH articles tutorial AND "machine learning"
```

### 2. 明確さのために括弧を使用

```
-- 明示的で読みやすい
SEARCH threads (golang OR python) AND (web OR api)

-- 理解しにくい
SEARCH threads golang OR python AND web OR api
```

### 3. 先頭のNOT演算子を避ける

```
-- 良い: 肯定的な用語を先に
SEARCH articles tech NOT old

-- 最適ではない: 先頭のNOT
SEARCH articles NOT old
```

先頭のNOTは除外前にすべてのドキュメントをスキャンする必要があります。

### 4. フィルタとテキスト検索を組み合わせる

```
-- 良い: フィルタで早期に結果を絞り込む
SEARCH articles tech FILTER category = ai FILTER status = 1

-- 動作するが効率は劣る
SEARCH articles tech AND ai AND published
```

インデックス化されたカラムのフィルタは、これらの用語のテキスト検索より高速です。

### 5. 大規模結果セットには常にLIMITを使用

```
-- 良い: partial_sort最適化を使用
SEARCH articles tech SORT created_at DESC LIMIT 10

-- 遅い: すべての結果の完全ソート
SEARCH articles tech SORT created_at DESC
```

### 6. 深いページネーションを最小化

```
-- 効率的
SEARCH articles tech LIMIT 10 OFFSET 0

-- 効率が劣る（大きなOFFSET）
SEARCH articles tech LIMIT 10 OFFSET 10000
```

深い結果にはカーソルベースのページネーションなどの代替戦略を検討してください。

---

## COUNTコマンド

すべてのブール演算クエリ構文は`COUNT`でも使用できます：

```
COUNT <table> <query_expression> [FILTER ...]
```

例：
```
COUNT threads (golang OR python) AND tutorial
COUNT articles tech FILTER status = 1 FILTER category = ai
COUNT posts database AND (mysql OR postgresql) NOT sqlite
```

**注意:** COUNTはSORT、LIMIT、OFFSETをサポートしません（カウントには不要）。

---

## 実装の詳細

### 文法（BNF）

クエリは適切な演算子優先順位を持つ抽象構文木（AST）に解析されます：

```bnf
query     → or_expr
or_expr   → and_expr (OR and_expr)*
and_expr  → not_expr (AND not_expr)*
not_expr  → NOT not_expr | primary
primary   → TERM | '(' or_expr ')'
```

### パフォーマンス特性

- **AND演算**: ソート済みポスティングリストを使用した効率的な積集合
- **OR演算**: 集合演算を使用した効率的な和集合
- **NOT演算**: すべてのドキュメントに対する補集合（潜在的に高コスト）
- **括弧**: パフォーマンスのオーバーヘッドなし。解析にのみ影響します

### N-gramトークン化

MygramDBはインデックス作成と検索にN-gramトークン化を使用します：

- **デフォルトN-gramサイズ**: 2（バイグラム） - テーブルごとに設定可能
- **CJKテキスト**: 漢字/かなに対して別のN-gramサイズ（設定可能）
- **Unicode正規化**: NFKC正規化、幅変換、オプションの小文字化

---

## 関連情報

- [プロトコルリファレンス](protocol.md) - すべてのコマンドとプロトコルの詳細
- [設定ガイド](configuration.md) - フィルタ、N-gramサイズ、制限の設定
- [パフォーマンスチューニング](performance.md) - 高度な最適化手法
- [README](../../README.md) - プロジェクト概要とクイックスタート
