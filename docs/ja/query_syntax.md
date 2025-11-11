# クエリ構文

MygramDBは、複雑なテキスト検索操作のための豊富なブール演算クエリ構文をサポートしています。

## 基本構文

### シンプルな検索
```
SEARCH <table> <term>
```
例：
```
SEARCH threads golang
```

### AND検索
```
SEARCH <table> term1 AND term2 AND term3
```
例：
```
SEARCH threads golang AND tutorial
```

### OR検索
```
SEARCH <table> term1 OR term2 OR term3
```
例：
```
SEARCH threads golang OR python OR rust
```

### NOT検索
```
SEARCH <table> term1 NOT term2
```
例：
```
SEARCH threads tutorial NOT beginner
```

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

### ネストした式
複数レベルの括弧をネストできます：

```
SEARCH <table> ((term1 OR term2) AND term3) OR term4
```

例：
```
SEARCH threads ((golang OR python) AND web) OR rust
```

### 複雑なクエリの例

#### 1. GoまたはPythonのチュートリアルを検索し、初心者向けコンテンツを除外
```
SEARCH threads (golang OR python) AND tutorial AND NOT beginner
```

#### 2. MySQLまたはPostgreSQLに関するデータベース記事を検索し、SQLiteを除外
```
SEARCH posts database AND (mysql OR postgresql) AND NOT sqlite
```

#### 3. PythonまたはRで機械学習の記事を検索し、TensorFlowを除外
```
SEARCH articles "machine learning" AND (python OR R) AND NOT tensorflow
```

## 演算子の優先順位

括弧が使用されていない場合、演算子は以下の優先順位を持ちます（高い順）：

1. **NOT** （最高）
2. **AND** （中）
3. **OR** （最低）

### 例

**クエリ:** `a OR b AND c`
**解釈:** `a OR (b AND c)`

**クエリ:** `NOT a AND b`
**解釈:** `(NOT a) AND b`

**クエリ:** `a AND b OR c AND d`
**解釈:** `(a AND b) OR (c AND d)`

## 引用符によるフレーズ検索

完全一致のフレーズ検索には、ダブルクォーテーションまたはシングルクォーテーションを使用します：

```
SEARCH <table> "hello world"
SEARCH <table> 'machine learning'
```

ブール演算子と組み合わせた例：
```
SEARCH threads "web framework" AND (golang OR python)
```

## フィルター条件

ブール演算クエリはフィルター条件と組み合わせることができます：

```
SEARCH <table> (golang OR python) AND tutorial FILTER status = published LIMIT 10
```

## ORDER BY句

ORDER BY句を使用して結果をソートできます：

```
SEARCH <table> <term> ORDER BY <column> [ASC|DESC]
```

### ソートオプション

#### プライマリキーでソート（デフォルト）
ORDER BYが指定されていない場合、結果はプライマリキーの降順でソートされます：

```
SEARCH threads golang
-- 以下と同等: SEARCH threads golang ORDER DESC
```

**プライマリキーソートのオプション：**

完全な構文：
```
SEARCH threads golang ORDER BY <primary_key> ASC
SEARCH threads golang ORDER BY <primary_key> DESC
```

**省略記法（推奨）：**
```
SEARCH threads golang ORDER BY ASC   -- プライマリキー昇順
SEARCH threads golang ORDER BY DESC  -- プライマリキー降順
SEARCH threads golang ORDER ASC      -- さらに短く（BYは省略可能）
SEARCH threads golang ORDER DESC     -- さらに短く（BYは省略可能）
```

#### フィルターカラムでソート
インデックス化された任意のフィルターカラムでソート：

```
SEARCH threads golang ORDER BY created_at DESC LIMIT 10
SEARCH posts database ORDER BY score ASC LIMIT 20
```

### 組み合わせの例

**複雑なブール式とORDER BY:**

パーサーが括弧とORを自動的に処理します - 引用符は不要です！

```
SEARCH threads (golang OR python) AND tutorial ORDER DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND database) NOT sqlite ORDER BY score ASC
SEARCH articles (python OR R) AND "機械学習" ORDER BY created_at DESC LIMIT 20
```

パーサーが括弧の深さを追跡するため、`()`内のキーワードは検索式の一部として扱われます。

**フィルターと組み合わせ:**
```
SEARCH threads (golang OR python) AND tutorial FILTER status = published ORDER BY created_at DESC LIMIT 10
SEARCH posts ((mysql OR postgresql) AND "こんにちは") FILTER category = database ORDER BY score ASC
```

**注意:**
- ORDER BYはすべてのクエリ形式とシームレスに動作：単純、AND/NOT、OR、入れ子の括弧
- パーサーが括弧の深さを自動的に追跡します
- 引用符で囲んだフレーズ（`"こんにちは"`）を括弧と混在させてもエスケープ不要です
- 特別な引用符やエスケープは不要です！

### パフォーマンスの考慮事項

**ソートアルゴリズム：**
- **LIMITあり**: `partial_sort`を使用 - O(N × log(K)) ただし K = LIMIT + OFFSET
- **LIMITなし**: 完全ソート - O(N × log(N))
- **メモリ**: インプレースソート、追加メモリ割り当てなし

**大規模な結果セット（例：100万件中80万件がヒット）の場合：**
- partial_sort最適化を活用するため、可能な限りLIMITを使用してください
- 数値プライマリキーのソートはフィルターカラムより高速です
- 結果はOFFSET/LIMIT適用**前に**ソートされます（正しいページネーション）

**パフォーマンス例：**
- 80万件の結果でLIMIT 100の場合：partial_sortで約3倍高速
- インプレースソート：メモリオーバーヘッドなし
- サンプルベースのカラム検証（最初の100件）：O(1)のオーバーヘッド

### カラム検証

- **プライマリキー**: 常に有効
- **フィルターカラム**: 少なくとも1つのドキュメントに存在する必要があります
- **存在しないカラム**: 警告としてログに記録され、NULL値として扱われます

## エラー処理

### 無効なクエリ

以下のクエリはエラーを返します：

- **空の括弧:** `()`
- **閉じられていない括弧:** `(golang AND python`
- **余分な閉じ括弧:** `golang AND python)`
- **オペランドのない演算子:** `AND`
- **末尾の演算子:** `golang AND`
- **閉じられていない引用符:** `"golang tutorial`

## COUNTコマンド

すべてのブール演算クエリ構文はCOUNTコマンドでも使用できます：

```
COUNT <table> (golang OR python) AND tutorial
```

## 実装の詳細

クエリは適切な演算子優先順位を持つ抽象構文木（AST）に解析されます。ASTはn-gramインデックスに対して評価され、マッチするドキュメントを返します。

### 文法（BNF）

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
- **NOT演算**: 全ドキュメントに対する補集合（大規模データセットでは高コスト）
- **括弧**: パフォーマンスのオーバーヘッドなし。解析にのみ影響します

## 最適なクエリのためのヒント

1. **制約の強い検索語を先に配置する** AND演算において
2. **複雑なクエリには括弧を使用する** 可読性向上のため
3. **可能な限り先頭のNOT演算子を避ける**（例：`NOT exclude`より`term AND NOT exclude`を推奨）
4. **可能な場合はフィルターを使用する** 全文検索の前に結果を絞り込む

## 関連情報

- [README.md](../../README.md) - プロジェクト概要
- [README_ja.md](../../README_ja.md) - プロジェクト概要（日本語）
- [Configuration Guide](../../examples/config.yaml) - サーバー設定
