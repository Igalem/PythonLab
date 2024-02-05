import requests
import json
from lxml import html
import infra.etl as etl
from datetime import date

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64)'
                    'AppleWebKit/537.36 (KHTML, like Gecko)'
                    'Chrome/44.0.2403.157 Safari/537.36'),
    'Accept-Language': 'en-US, en;q=0.5'
}

HOST_ARGS = {
            'host' : 'localhost',
            'user' : 'root',
            'password' : 'root',
            'database' : 'ikea'
            }

etl = etl.ETL(**HOST_ARGS)

MRR_TABLE = 'mrr_products'
TRG_TABLE = 'products'

def send_post_request(input=None):
    url = 'https://sik.search.blue.cdtapps.com/il/he/search?c=listaf&v=20231027'
    payload = {
        "searchParameters": {
            "input": input,
            "type": "CATEGORY"
        },
        "store": "206",
        "isUserLoggedIn": False,
        "partyUId": "",
        "components": [
            {
                "component": "PRIMARY_AREA",
                "columns": 4,
                "types": {
                    "main": "PRODUCT",
                    "breakouts": ["PLANNER", "LOGIN_REMINDER"]
                },
                "filterConfig": {
                    "max-num-filters": 4
                },
                "sort": "RELEVANCE",
                "window": {
                    "offset": 12,
                    "size": 48
                }
            }
        ]
    }

    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, data=json.dumps(payload), headers=headers)

    if response.status_code == 200:
        # print("Request was successful.")
        return response
    else:
        # print("Request failed.")
        return response.status_code

def create_session(url, headers=None):
    session = requests.Session()
    page_content = session.get(url=url, headers=headers)  ## Add proxies
    if page_content.ok == False:
        print(page_content)
        exit()
    return page_content


def content_to_tree(page_content):
    tree = html.fromstring(page_content.content)
    return tree


if __name__ == "__main__":

    ## delete current date data from mrr_products:
    etl.exec_script(script_dir='scraping/ikea/sql/mrr_products.sql')
    
    catalog_url = 'https://www.ikea.com/il/he/cat/products-products/'

    page_content = create_session(url=catalog_url, headers=HEADERS)
    tree = content_to_tree(page_content=page_content)

    xpaths = tree.xpath("//div[@class='vn-p-grid-gap vn-accordion__item']/ul[@class='vn-list--plain vn-list vn-accordion__content']//a[@class='vn-link vn-nav__link']//@href")
    # xpaths = ['https://www.ikea.com/il/he/cat/sofas-fu003/']
    
    for xpath in list(set(xpaths)):
        data = []
        url_content = xpath[:-1]
        # print(f"xpath = {xpath}")
        entity = url_content.split('/')[-1]
        entity_code_page = entity.split('-')[-1]
        print(entity, entity_code_page)

        page_content = create_session(url=url_content, headers=HEADERS)
        tree = content_to_tree(page_content=page_content)

        try:
            category = tree.xpath("//h1[@class='plp-page-title__title']//text()")[0]
            # print(category)
            try:
                total_products = tree.xpath("//span[@class='plp-filter-information__total-count']//text()")[0].split(" ")[0]
                print(f"Total products: {total_products}")
            except:
                total_products = None

            if total_products:

                # url = f'https://sik.search.blue.cdtapps.com/il/he/product-list-page/more-products?category={entity_code_page}&start=0&end={total_products}&c=lf&v=20220826&sort=RELEVANCE&sessionId=be0e0e39-9e28-4ad2-b212-67251f8a66c7'
                # url = 'https://sik.search.blue.cdtapps.com/il/he/search?c=listaf&v=20231027'

                res = send_post_request(input=entity_code_page)
                content = res.json()
                products = content['results'][0]['items']
                created = date.today()

                for prod in products:
                    product = prod['product']
                    # print(product)
                    category = category
                    product_number = product['itemNoGlobal']
                    product_name = product['name']
                                        
                    try:
                        product_desc = product['mainImageAlt']
                    except:
                        product_desc = None
                    
                    type = product['itemType']
                    type_name = product['typeName']
                    variants = product['gprDescription']['numberOfVariants']

                    try:
                        colors = product['colors'][0]['name']
                    except:
                        colors = None

                    price = product['salesPrice']['current']['wholeNumber'].replace(',', '')

                    try:
                        prev_price = product['salesPrice']['previous']['wholeNumber'].replace(',', '')
                    except:
                        prev_price = None

                    try:
                        units = product['salesPrice']['priceUnit']
                    except:
                        units = 1

                    url = product['pipUrl']
                    tag = product['tag']
                    online_sell = product['onlineSellable']
                    last_chance = product['lastChance']
                    try:
                        availability = product['availability'][0]['prefix']
                    except:
                        availability = None
                    
                    is_breath_taking = product['salesPrice']['isBreathTaking']

                    data.append([entity, category, product_number, product_name, product_desc, \
                                    type, type_name, variants, colors, price, prev_price, units, url, tag, online_sell, last_chance, availability,\
                                    is_breath_taking, created])
                    
                    # data.append([entity])
                    
                # print(data[97])
                # for i in range(100):
                #     try:
                #         etl.insert_bulk(table=MRR_TABLE, truncate='n', data=[data[i]])
                #     except:
                #         print('x')

                etl.insert_bulk(table=MRR_TABLE, truncate='n', data=data)


        except:
            print(f'{xpath} --- Not loaded!! ----------- \n')
            continue

    ## populate products 
    etl.exec_script(script_dir='scraping/ikea/sql/products.sql')

    ## populate category_daily
    etl.exec_script(script_dir='scraping/ikea/sql/category_daily.sql')