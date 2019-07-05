## 依存パッケージと依存ソフトウェア

GitHubのコードを参照してください
[https://github.com/GINK03/minimal-search-engine:embed]

様々なサイトを巡回する必要があり、requestsが文字コードの推論を高確率で失敗するので、`nkf` をlinux環境で入れている必要があります。
```console
$ sudo apt install nkf
$ which nkf
/usr/bin/nkf
```
Mecabも入れます
```
$ sudo apt install mecab libmecab-dev mecab-ipadic
$ sudo apt install mecab-ipadic-utf8
$ sudo apt install python-mecab
$ pip3 install mecab-python3
$ git clone --depth 1 https://github.com/neologd/mecab-ipadic-neologd.git
$ ./bin/install-mecab-ipadic-neologd -n
```
残りの依存をインストールします
```console
$ pip3 install -r requirements.txt
```

## 再現
基本的にGitHubのコードをUbuntu等のLinuxでAから順に実行してもらえば、再現できます。  

クローラ（スクレイパー）はやろうと思えば無限に取得してしまうので、適当にSEEDを決めて、適当な時間で終了することを前提としていています。

## 全体の処理の流れ
 A. クローリング  
 B. クローリングしたHTMLを, title, description, body, hrefsをパースしデータを整形する  
 C. IDF辞書の作成  
 D. TFIDFのデータを作成  
 F. 転置したurl, hrefの対応を作成(単純な被参照量の特徴量)  
 G. 非参照数のカウントと、PageRankのための学習データの作成  
 H. URLとtfidfのウェイトの転置インデックスを作成  
 I. hash化されたURLと実際のURLの対応表の作成  
 J. PageRankの学習  
 K. 検索のインターフェース   

<div align="center">
 <img width="650px" src="https://user-images.githubusercontent.com/4949982/60479460-e28c2500-9cc0-11e9-890c-def6f32a17d9.png">
</div>

## プログラムの詳細
### A. クローリング
 特定のドメインによらず、網羅的にクローリングしていきます。 
 ブログサイトをシードとしてドメインを限定せずどこまでも深く潜っていきます。  
 多様なサイトをクロールするがとても重いので、[自作した分散可能なKVSをバックエンドのDB](https://github.com/GINK03/pure-python-kvs-f2db)と利用しています。SQLLiteなどだとファイルが壊れやすく、LevelDB等だとシングルアクセスしかできません。  

<div align="center">
 <img width="650px" src="https://user-images.githubusercontent.com/4949982/60482435-78797d00-9ccc-11e9-83f1-2623c2878dc8.png">
</div>

### B. HTMLのパースと整形
 Aで取得したデータは大きすぎるので、Bのプロセスで、tfidfでの検索の主な特徴量となる、"title", "description", "body"を取り出します。  
 また、そのページが参照している外部のURLをすべてパースします。  
```python
soup = BeautifulSoup(html, features='lxml')
for script in soup(['script', 'style']):
    script.decompose()
title = soup.title.text
description = soup.find('head').find(
                'meta', {'name': 'description'})
if description is None:
    description = ''
else:
    description = description.get('content')
body = soup.find('body').get_text()
body = re.sub('\n', ' ', body)
body = re.sub(r'\s{1,}', ' ', body)
```
BeautifulSoupでシンプルに処理することができる.  

### C. IDF辞書の作成 
 頻出する単語の重要度を下げるために、各単語がどの程度のドキュメントで参照されているかをカウントします。  

### D. TDIDFの計算
 B, Cのデータを利用して、TFIDFとして完成させます  
`title` `description` `body`はそれぞれ重要度が異なっており、 `title` : `description` : `body` = `1` : `1` : `0.001`  
として処理しました。  
```python
# title desc weight = 1
text = arow.title + arow.description 
text = sanitize(text)
for term in m.parse(text).strip().split():
    if term_freq.get(term) is None:
        term_freq[term] = 0
    term_freq[term] += 1

# title body = 0.001 
text = arow.body
text = sanitize(text)
for term in m.parse(text).strip().split():
    if term_freq.get(term) is None:
        term_freq[term] = 0
    term_freq[term] += 0.001 # ここのweightを 0.001 のように小さい値を設定する
```

### F. あるURLと、あるURLのHTMLがリンクしているURLの転置インデックスを作成
 昔良くあった、URLのリンクを色んな所から与えるとSEOができるということを知っていたので、どの程度外部から被参照されているか知るため、このような処理を行います

### G. 被参照数のカウントと、PageRankのための学習データの作成
 Fで作成したデータをもとに、networkxというライブラリでPageRankのノードのウェイトを学習可能なので、学習用データを作成します  
 
 このようなデータセットが入力として望まれます(右のハッシュがリンク元、左のハッシュがリンク先)  
```
d2a88da0ca550a8b 37a3d49657247e61
d2a88da0ca550a8b 6552c5a8ff9b2470
d2a88da0ca550a8b 3bf8e875fc951502
d2a88da0ca550a8b 935b17a90f5fb652
7996001a6e079a31 aabef32c9c8c4c13
d2a88da0ca550a8b e710f0bdab0ac500
d2a88da0ca550a8b a4bcfc4597f138c7
4cd5e7e2c81108be 7de6859b50d1eed2
```

### H. 単語から簡単にURLを逆引きできるように、転置インデックスの作成
 最もシンプルな単語のみでの検索に対応できるように、単語からURLをすぐ検索できるindexを作ります  
出力が、単語（のハッシュ値）ごとにテキストファイルが作成されて、 `URLのハッシュ` , `weight(tfidf)` , `refnum(被参照数)` のファイルが具体的な転置インデックスのファイルになります
```
0010c40c7ed2c240        0.000029752     4
000ca0244339eb34        0.000029773     0
0017a9b7d83f5d24        0.000029763     0
00163826057db7c3        0.000029773     0
```

### I. URLとhash値の対応表の作成
 URLはそのままメモリ上に持つとオーバーフローしてしまうので、sha256をつかって先頭の16文字だけを使った小さいhash値とみなすことで100万オーダーのドキュメントであってもある程度実用に耐えうる検索が可能になります。

### J. PageRankの学習
 Gで作成したデータを学習してURLにPageRankの値を学習します。
 
 networkxを用いれば凄くシンプルなコードで学習する事ができます  
 
```
import networkx as nx
import json
G = nx.read_edgelist('tmp/to_pagerank.txt', nodetype=str)
# ノード数とエッジ数を出力
print(nx.number_of_nodes(G))
print(nx.number_of_edges(G))
print('start calc pagerank')
pagerank = nx.pagerank(G)
print('finish calc pagerank')
json.dump(pagerank, fp=open('tmp/pagerank.json', 'w'), indent=2)
```
 
### K. 検索のインターフェース
 検索IFを提供
 
```console 
$ python3 K001_search_query.py
(ここで検索クエリを入力)
```
例
```console
$ python3 K001_search_query.py
ふわふわ
                   hurl    weight  refnum  weight_norm                                                            url  pagerank  weight*refnum_score+pagerank
9276   36b736bccbbb95f2  0.000049       1     1.000000  https://bookwalker.jp/dea270c399-d1c5-470e-98bd-af9ba8d8464a/  0.000146                      1.009695
2783   108a6facdef1cf64  0.000037       0     0.758035     http://blog.livedoor.jp/usausa_life/archives/79482577.html  1.000000                      0.995498
32712  c3ed3d4afd05fc43  0.000045       1     0.931093          https://item.fril.jp/bc7ae485a59de01d6ad428ee19671dfa  0.000038                      0.940083
...
```

## 実際の使用例
"雷ちゃん"等で検索すると、ほしい情報がおおよそちゃんと上に来るようにチューニングすることができました。  
Pixivについては明示的にクローリング先に設定していないが、Aのクローラがどんどんとリンクをたどりインデックスを作成した結果で、自動で獲得したものです。  

<div align="center">
 <img width="100%" src="https://user-images.githubusercontent.com/4949982/60481343-ec655680-9cc7-11e9-8a95-95d1562190ca.png">
</div>

"洒落怖"など、他のクエリでも望んだ結果が帰ってきています。
<div align="center">
 <img width="100%" src="https://user-images.githubusercontent.com/4949982/60481649-4286c980-9cc9-11e9-9601-2cb562ca29a9.png">
</div>

## 検索のスコアリングはどうあるべきか

手でいろいろ試してみて、良さそうなスコアはこのような感じなりました。（私が正解データです）  
<div align="center">
 <img width="500px" src="https://user-images.githubusercontent.com/4949982/60492003-fc3f6380-9ce4-11e9-8374-2527be25a90c.png">
</div>
