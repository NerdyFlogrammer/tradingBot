import requests
from bs4 import BeautifulSoup

if __name__ == '__main__':
    uid = 52000
    html = requests.get("https://www.dpa-afx.de/news/?uid={uid}".format(uid=uid))
    parsed_html = BeautifulSoup(html.text, "html.parser").body.find('div', attrs={'class':'tx-rmtwitter-pi1'})
    date_time = parsed_html.find('span', attrs={'class':'news-list-date'})
    titel = parsed_html.find('h2').text
    text = parsed_html.text
    start = titel
    end = '--- '
    s = parsed_html.text
    text =s[s.find(start) + len(start):s.rfind(end)].strip()

    print(text)