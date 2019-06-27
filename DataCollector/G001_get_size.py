from pathlib import Path
import pickle
href_urls = {}
for path in list(Path().glob('./tmp/inverted/*'))[:10]:
    print(path)
    _href_urls = pickle.load(path.open('rb'))
    for href, urls in _href_urls.items():
        if href not in href_urls:
            href_urls[href] = set()
        href_urls[href] |= urls

for href, urls in sorted(href_urls.items(), key=lambda x:len(x[1])*-1):
    print(href, len(urls), urls)

