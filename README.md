# Pure Pythonでサーチエンジン(PageRank, tfidfとか対応)

## 全体の処理の流れ
 - A. クローリング
 - B. クローリングしたHTMLを, title, description, body, hrefsをパースしデータを整形する
 - C. IDF辞書の作成
 - D. TFIDFのデータを作成
 - F. 転置したurl, hrefの対応を作成(単純な非参照量の特徴量)
 - G. 非参照数のカウントと、PageRankのための学習データの作成
 - H. 単語とURLとtfidfのウェイトの転置インデックスを作成
 - I. hash化されたURLと実際のURLの対応表の作成
 - J. PageRankの学習
 - K. 検索のインターフェース 
