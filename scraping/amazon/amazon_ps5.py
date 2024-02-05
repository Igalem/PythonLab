import requests
from lxml import html

HEADERS = {
    'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64)'
                    'AppleWebKit/537.36 (KHTML, like Gecko)'
                    'Chrome/44.0.2403.157 Safari/537.36'),
    'Accept-Language': 'en-US, en;q=0.5'
}



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


def parser(tree):
    try:
        elements = tree.xpath("//div[@class='sg-col-20-of-24 s-result-item s-asin sg-col-0-of-12 sg-col-16-of-20 sg-col s-widget-spacing-small sg-col-12-of-16']")
    except:
        elements = tree.xpath("//div[@class='sg-col-4-of-24 sg-col-4-of-12 s-result-item s-asin sg-col-4-of-16 AdHolder sg-col s-widget-spacing-small sg-col-4-of-20']")
    
    for element in elements:
        price = element.xpath(".//span[@class='a-offscreen']//text()")
        owner = element.xpath(".//div[@class='a-row']//span//text()")
        name =  element.xpath(".//span[@class='a-size-medium a-color-base a-text-normal']//text()")
        href = element.xpath(".//a[@class='a-link-normal s-underline-text s-underline-link-text s-link-style a-text-normal']/@href")
        link = f"https://www.amazon.com{href[0]}".split()
        item_no = element.xpath("@data-asin")

        post_dict = {"name": name[0],
                    "price": price,
                    "item_no": item_no[0], 
                    "owner": owner,
                    "link": link[0] }

        posts.append(post_dict)
    return posts

if __name__ == "__main__":
    posts = []
    for i in range(1,2):
        print(f"----------------page {i} ----------------\n")
        url = f"https://www.amazon.com/s?k=playstation+5&page={i}"
        print(url)
        page_content = create_session(url=url, headers=HEADERS)

        tree = content_to_tree(page_content=page_content)
        posts = parser(tree=tree)

    for post in posts:
        if post["price"] and 'by PlayStation' in ''.join(post['owner']):
            print(f"Name: {post['name']}")
            print(f"Price: {post['price']}")
            print(f"Owner: {post['owner']}\n")