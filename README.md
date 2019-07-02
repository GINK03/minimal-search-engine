# Pure Pythonでサーチエンジン(PageRank, tfidfとか対応)

## 全体の処理の流れ
 - 1. クローリング
 - 2. クローリングしたHTMLを, title, description, body, hrefsをパースしデータを整形する
 - 3. IDF辞書の作成
 - 4. TFIDFのデータを作成
 - 5. 転置したurl, hrefの対応を作成(単純な非参照量の特徴量)
 - 6. 非参照数と、PageRankのための学習データの作成
 - 7. 単語とURLとtfidfのウェイトの転置インデックスを作成
 - 8. hash化されたURLと実際のURLの対応表の作成
 - 9. PageRankの学習
 
