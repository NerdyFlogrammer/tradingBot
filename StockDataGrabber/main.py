from datetime import datetime
import psycopg2
import requests as requests

source = "Consorsbank"
stock_exchange = "Xetra"


if __name__ == '__main__':
    start = datetime.today().strftime('%Y-%m-%d') + 'T06:00:00.000Z'
    end = datetime.today().strftime('%Y-%m-%d') + 'T19:00:00.000Z'
    conn = psycopg2.connect(host='192.168.0.247',
                            port=5432,
                            user='postgres',
                            password='stockspw',
                            database='stocksdb')
    cursor = conn.cursor()
    cursor.execute("""SELECT s.name, s.isin, sm.data_src_stock_id FROM stock s 
    INNER JOIN source_metadata sm  ON s.id = sm.stock_id 
    INNER JOIN stock_exchange se ON sm.stock_exchange_id = se.id 
    INNER JOIN data_source ds ON ds.id = sm.data_source_id 
    WHERE ds.id = (SELECT ID FROM data_source WHERE NAME = %s) AND 
    se.id =(SELECT ID FROM stock_exchange WHERE NAME = %s);""", (source, stock_exchange))
    stocks = cursor.fetchall()
    cursor.execute("""select url from data_source where name=%s;""", (source,))
    base_url = cursor.fetchone()[0]
    for stock in stocks:
        url = base_url.format(id=stock[2], start=start, end=end)
        items = requests.get(url).json()[0]['TimesSalesV1']['ITEMS']
        items.sort(key=lambda i: (i['DATETIME_PRICE'], i['TOTAL_VOLUME']))
        cursor = conn.cursor()
        for item in items:
            cursor.execute("INSERT INTO stock_data "
                           "(date_time, price, total_volume, volume, stock_id, stock_exchange_id) "
                           "VALUES(%s, %s, %s, %s, (SELECT id from stock WHERE isin=%s), "
                           "(SELECT id FROM stock_exchange WHERE name=%s))",
                           (item['DATETIME_PRICE'], item['PRICE'],
                            item['TOTAL_VOLUME'], item['VOLUME'], stock[1], stock_exchange))
            conn.commit()
        print("{date} insert {share} prices".format(share=stock[0], date=datetime.today().strftime('%Y-%m-%d')))
    cursor.close()
    conn.close()
