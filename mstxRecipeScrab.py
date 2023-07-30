import requests
from bs4 import BeautifulSoup
import collections
import random
import warnings
import re
import pickle
from IPython.core.interactiveshell import InteractiveShell

warnings.filterwarnings("ignore")

InteractiveShell.ast_node_interactivity = "all" 



my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11",
    "Opera/9.25 (Windows NT 5.1; U; en)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12",
    "Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9",
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 ",
]  



search_keyword_dict = collections.defaultdict(list)
for idx, menu in tqdm(enumerate(df_result_gpt.menu_name.tolist())):
    if idx % 50 == 0:
        print(f"{menu} start...")
    url = f"https://home.meishichina.com/search/recipe/{menu}/"

    response = requests.get(
        url,
        headers={"User-Agent": random.choice(my_headers)},
        verify=False,
    )
    soup = BeautifulSoup(response.text, "html.parser")

    recipe_list = (
        soup.find("div", {"class": "ui_list_1"}).find_all("li")
        if soup.find("div", {"class": "ui_list_1"})
        else None
    )
    if recipe_list:
        for recipe in recipe_list:
            recipe_link = recipe.find("a").get("href")
            recipe_menu_food = recipe.find("p", class_="subcontent").text
            recipe_name = recipe.find("em").text if recipe.find("em") else ""
            recipe_content = recipe.find("span").text if recipe.find("span") else ""
            recipe_name_full = recipe.find("h4").text
            search_keyword_dict[menu].append(
                {
                    "recipe_link": recipe_link,
                    "recipe_menu_food": recipe_menu_food,
                    "recipe_name": recipe_name,
                    "recipe_name_full": recipe_name_full,
                }
            )
    if idx % 50 == 0:
        print(f"{menu} end.")  


with open('search_keyword_dict20230706.pickle', 'wb') as file:
    pickle.dump(dict(search_keyword_dict), file) 
