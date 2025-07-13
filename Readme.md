編譯器理論（Compiler Theory）中的「語義保持轉換」（Semantics-Preserving Transformation）是完全一致的。我們不能比較程式運行的「結果」，所以必須比較程式本身的「結構與語義」。

將程式碼從純文字（plain text）轉換為一種能夠代表其邏輯結構的抽象語法樹（Abstract Syntax Tree, AST），然後比較這兩個物件的等價性。

sqlglot是python一個功能極其強大的 SQL 解析器、轉譯器、優化器與引擎。它原生支援多種 SQL 方言（包含 Oracle 和 Trino），並且其核心設計就是圍繞著 AST 進行操作。
