from trino.dbapi import connect
from trino.auth import BasicAuthentication

import urllib3 
import datetime 
from datetime import timezone
import pathlib


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

current_dateTime = datetime.datetime.now(timezone.utc)
#print(utc.localize(current_dateTime))

conn = connect(
    user="dana.day",
    auth=BasicAuthentication("dana.day", "Tr0mboneS4xophoneB0ss"),
    host = "https://trino-test.evodian.au",
    port = "443",
    catalog = "vast",
    schema = "tesseract",
    verify = False,
)
cur = conn.cursor()

types = {'raw_customers': {'id': 'varchar', 'name': 'varchar'}, 
         'raw_items': {'id': 'varchar', 'orderID': 'varchar', 'sku': 'varchar'},
         'raw_orders': {'id': 'varchar', 'customer': 'varchar', 'ordered_at': 'date', 'store_id': 'varchar', 'subtotal': 'int', 'tax_paid': 'int', 'order_total': 'int'},
         'raw_products': {'sku': 'varchar', 'name': 'varchar', 'type': 'varchar', 'price': 'int', 'description': 'varchar'},
         'raw_stores': {'id': 'varchar', 'name': 'varchar', 'opened_at': 'date', 'tax_rate': 'int'},
         'raw_supplies': {'id': 'varchar', 'name': 'varchar', 'cost': 'int', 'perishable': 'boolean', 'sku': 'varchar'}}


def main():
    #cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".pricehistemp')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_customers')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_items')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_orders')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_products')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_stores')
    cur.execute('drop table IF EXISTS vast."tesseract/danadbtest".raw_supplies')
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_customers(
                "id"   varchar,
                "name" varchar
    )""")
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_items(
                "id"   varchar,
                "orderID" varchar,
                "sku" varchar
    )""")
    #id,customer,ordered_at,store_id,subtotal,tax_paid,order_total
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_orders(
                "id"   varchar,
                "customer" varchar,
                "ordered_at" date,
                "store_id"  varchar,
                "subtotal" int,
                "tax_paid" int,
                "order_total" int
    )""")
#sku,name,type,price,description
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_products(
                "sku"   varchar,
                "name" varchar,
                "type" varchar,
                "price"  int,
                "description" varchar
    )""")
#id,name,opened_at,tax_rate
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_stores(
                "id"   varchar,
                "name" varchar,
                "opened_at" date,
                "tax_rate"  int
    )""")
#id,name,cost,perishable,sku
    cur.execute("""CREATE TABLE vast."tesseract/danadbtest".raw_supplies(
                "id"   varchar,
                "name" varchar,
                "cost" int,
                "perishable"  boolean,
                "sku" varchar
    )""")
    all = cur.fetchall()
    #there are only 69 active regions 3 of which are dev only with the current state of eve development, when more arre appriciated this will have to change. Luckily eve gives the regions that exist in order, and they seem to continue this pattern into the future
    names = types.keys()
    for name in names:
        getsqled(name)
    print('done')




def getsqled(filename):
    #H:\GitRepos\jaffels-bbg\jaffle-data
    path = pathlib.WindowsPath('H:/', 'GitRepos', 'jaffels-bbg', 'jaffle-data', str(filename+ '.csv'))
    with open(path, 'r') as f:
            file = f.read()
            fii = file.splitlines()
            #print(fii)
            key = types[filename]
            totes = []
            formats = key.keys()
            for i in range(len(fii)-1):
                line = fii[i+1]
                values = line.split(',')
                formats = list(key.keys())
                fum = []
                if len(values) > len(list(key.values())):
                    extra = str((',').join(values[4:])).replace("'", "''")
                    values = list(values[:4]) 
                    values.append(extra)
                    if len(values) != len(list(key.values())):
                        break
                for x in range (len(values)):
                    a = key[formats[x]]
                    if a == 'varchar':
                        fum.append(f"'{values[x]}'")
                    if a == 'date':
                        #2016-09-23T16:40:00 to TIMESTAMP '2016-09-23 16:40:00'
                        datio = values[x].replace('T', ' ')
                        fum.append(f""" timestamp '{datio}'""")
                    if a == 'int':
                        fum.append(values[x])
                    if a == 'boolean':
                        fum.append(values[x])
                totes.append(f"({','.join(fum)})")
                if len(totes) > 300:
                    sendtotrino(filename, formats, totes)
                    totes = []
            sendtotrino(filename, formats, totes)
                #con.commit()

def sendtotrino(filename, formats, totes):
    columns = f"""{filename}({','.join(formats)}) """
    total = ','.join(totes)
    #print(f"""INSERT INTO vast."tesseract/danadbtest".{columns} VALUES {total}""")
    cur.execute(f"""INSERT INTO vast."tesseract/danadbtest".{columns} VALUES {total}""")


main()
#format("ABS:BA_SA2_201116(1.0.0)', '2', '9', 'TOT', 'TOT', 'SA3', '31201', 'M', '2016-06', '1307', 'AUD', '3', '', '")
print('done')
