import pandas as pd
import MeCab


if __name__ == '__main__':
    m = MeCab.Tagger('-Owakati')
    
    text = input()
    for term in m.parse(text).strip().split():
        print(term)
