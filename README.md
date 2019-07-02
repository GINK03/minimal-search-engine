# Pure Pythonでサーチエンジン(PageRank, tfidfとか対応)

## 全体の処理の流れ
 - A. クローリング
 - B. クローリングしたHTMLを, title, description, body, hrefsをパースしデータを整形する
 - C. IDF辞書の作成
 - D. TFIDFのデータを作成
 - F. 転置したurl, hrefの対応を作成(単純な非参照量の特徴量)
 - G. 非参照数のカウントと、PageRankのための学習データの作成
 - H. URLとtfidfのウェイトの転置インデックスを作成
 - I. hash化されたURLと実際のURLの対応表の作成
 - J. PageRankの学習
 - K. 検索のインターフェース 

<div align="center">
 <img width="650px" src="https://user-images.githubusercontent.com/4949982/60479460-e28c2500-9cc0-11e9-890c-def6f32a17d9.png">
</div>

## プログラムの詳細
### A. クローリング
 特定のドメインによらず、網羅的にクローリングしていきます。 このときブログサイトをシードとしてドメインを限定せずどこまでも深く潜っていきます。  
 このとき、多様なサイトをクロールするがとても重いので、[自作した分散可能なKVSをバックエンドのDB](https://github.com/GINK03/pure-python-kvs-f2db)と利用しています。SQLLiteなどだとファイルが壊れやすく、LevelDB等だとシングルアクセスしかできません。  
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
 昔良くあった、URLのリンクを色んな所から与えるとSEOができるということを知っていたので、どの程度外部から非参照されているか知るため、このような処理を行います

### G. 非参照数のカウントと、PageRankのための学習データの作成
 Fで作成したデータをもとに、networkxというライブラリでPageRankのノードのウェイトを学習可能なので、学習用データを作成します  

### H. tfidfから簡単にURLを逆引きできるように、転置インデックスの作成
 最もシンプルなtfidfのみでの検索に対応できるように、単語からURLをすぐ検索できるindexを作ります  

### I. URLとhash値の対応表の作成
 URLはそのままメモリ上に持つとオーバーフローしてしまうので、sha256をつかって先頭の16文字だけを使った小さいhash値とみなすことで700万件程度の実用に耐えうる検索が可能になります。

### J. PageRankの学習
 Gで作成したデータを学習してURLにウェイトを割り振ります
 
### K. 検索のインターフェース
 検索IFを提供

## 実際の使用例
"雷ちゃん"等で検索すると、ほしい情報がおおよそちゃんと上に来るようにチューニングすることができた。  
Pixivについては明示的にクローリング先に設定していないが、Aのクローラがどんどんとリンクをたどりインデックスを作成した結果で、自動で獲得したものである。  

<div align="center">
 <img width="100%" src="https://user-images.githubusercontent.com/4949982/60481343-ec655680-9cc7-11e9-8a95-95d1562190ca.png">
</div>

"洒落怖"など、他のクエリでも望んだ結果が帰ってきている。
<div align="center">
 <img width="100%" src="https://user-images.githubusercontent.com/4949982/60481649-4286c980-9cc9-11e9-9601-2cb562ca29a9.png">
</div>
