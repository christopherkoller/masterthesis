import requests
from bs4 import BeautifulSoup
from selenium import webdriver, common

import itertools
import random
import re
import multiprocessing
import json
import os
import time
import gc

import traceback

OUTPUT_PATH = r'D:\UNIGIS\03__Masterarbeit\03_geodaten\scrawler_output_180224_2'

CHROMEDRIVER = r"J:\UNIGIS\03__Masterarbeit\02_geodata_tools\chromedriver_2.35\chromedriver.exe"

EMPTY_HTML = u'<html xmlns="http://www.w3.org/1999/xhtml">' \
             u'<head></head>' \
             u'<body></body>' \
             u'</html>'

PAGE_LIMIT = 100

PROXIES = ()
PROXIES_TIMESTAMP = -1

ALIAS = {
    'www.immobilienscout24.at': r'immoscout24',
    'immo.sn.at': r'immo_sn',
    'www.willhaben.at': 'willhaben',
    'www.wohnnet.at': r'wohnnet'
}

IFRAMES = {
    'immo.sn.at': {'css selector': '#checkmyplace > iframe'},
}

WHITE_LIST = {
    'www.immobilienscout24.at': r'expose/[0-9a-f]{24}',
    'immo.sn.at': r'^((?!.*service.*)).*(-[1-9][0-9A-Z]{5})',
    'www.willhaben.at': r'-\d{8,10}',
    'www.wohnnet.at': r'^((?!.*business.*)).*-\d{8,10}'
}


LOAD_VIA = {
    'www.immobilienscout24.at': 'selenium',
    'immo.sn.at': r'selenium',
    'www.willhaben.at': r'selenium',
    'www.wohnnet.at': r'requests'
}

SEED_LIST = {
    'www.immobilienscout24.at': {
        'url': '''
            https://www.immobilienscout24.at/resultlist?
                useType=RESIDENTIAL
                &region=005
                &sort=LATEST
                &page={page}
        ''',
        'page': range(1, PAGE_LIMIT)
    },

    'immo.sn.at': {
        'url': '''
            https://immo.sn.at/suchergebnisse?
                s=relevance
                &l=Salzburg+%28Bundesland%29
                &r=10km
                &usageType={usage_type}
                &t=all%3A{rental_sale}
                &pf=&pt=&rf=0&rt=0&sf=&st=&yf=&yt=&ff=&ft=&o=&u=
                &page={page}
                ''',
        'usage_type': ('private', 'commercial'),
        'rental_sale': ('rental', 'sale'),
        'page': range(1, PAGE_LIMIT)

    },

    'www.willhaben.at': {
        'url': '''
            https://www.willhaben.at/iad/immobilien/{dir}?
                areaId=5&
                page={page}&
                view=
        ''',
        'dir': (
            "haus-kaufen/haus-angebote",
            "haus-mieten/haus-angebote",
            "grundstuecke/grundstueck-angebote",
            "eigentumswohnung/eigentumswohnung-angebote",
            "mietwohnungen/mietwohnung-angebote",
            "gewerbeimmobilien-kaufen/gewerbeimmobilien-angebote",
            "gewerbeimmobilien-mieten/gewerbeimmobilien-angebote",
            "ferienimmobilien-kaufen/ferienimmobilien-angebote",
            "ferienimmobilien-mieten/ferienimmobilien-angebote"
        ),
        'page': range(1, PAGE_LIMIT)

    },
    'www.wohnnet.at': {
        'url': '''
            https://www.wohnnet.at/immobilien/salzburg/?
                seite={page}
        ''',
        'page': range(1, PAGE_LIMIT)
    },
}


class HtmlLoadException(Exception):
    pass

class NoProxysError(Exception):
    pass


def convert_seconds(seconds):

    m, s = divmod(float(seconds), 60)
    h, m = divmod(m, 60)

    return "{0:02.0f}:{1:02.0f}:{2:07.4f}".format(h, m, s)


def populate_seed_list():

    seed_list = []

    for base_url in SEED_LIST:

        keys = SEED_LIST[base_url].keys()
        keys.remove('url')

        vals = []

        for key in keys:
            vals.append(SEED_LIST[base_url][key])
        vals = list(itertools.product(*vals))

        for val in vals:
            val = dict(zip(keys, val))
            seed_page = SEED_LIST[base_url]['url'].format(**val)
            seed_page = ''.join(seed_page.strip().split())
            seed_list.append(seed_page)

    random.shuffle(seed_list)

    return seed_list


def refresh_proxies(proxy_level='elite proxy'):

    global PROXIES, PROXIES_TIMESTAMP
    PROXIES = []

    eu_countries = {
        u'BE': u'Belgien',
        u'FR': u'Frankreich',
        # u'BG': u'Bulgarien',
        # u'DK': u'D\xe4nemark',
        u'HR': u'Kroatien',
        u'DE': u'Deutschland',
        u'HU': u'Ungarn',
        # u'FI': u'Finnland',
        u'NL': u'Niederlande',
        #u'PT': u'Portugal',
        # u'LV': u'Lettland',
        # u'LT': u'Litauen',
        u'LU': u'Luxemburg',
        # u'RO': u'Rum\xe4nien',
        # u'PL': u'Polen',
        # u'EL': u'Griechenland',
        # u'EE': u'Estland',
        u'IT': u'Italien',
        u'CZ': u'Tschechische Republik',
        # u'CY': u'Zypern',
        u'AT': u'\xd6sterreich',
        # u'IE': u'Irland',
        # u'ES': u'Spanien',
        u'SK': u'Slowakei',
        # u'MT': u'Malta',
        u'SI': u'Slowenien',
        # u'UK': u'Vereinigtes K\xf6nigreich',
        # u'SE': u'Schweden'
    }



    url = "https://free-proxy-list.net/"
    page_source = requests.get(url).text
    soup = BeautifulSoup(page_source)

    columns = []
    for column in soup.find_all("thead")[0].find_all("th"):
        columns.append(column.text)

    for row in soup.find_all("tbody")[0].find_all("tr"):

        values = dict(zip(columns, [data.text for data in row]))

        if values["Code"] in eu_countries.keys() and \
           values["Anonymity"] == proxy_level and \
           values["Https"] == 'yes':

            PROXIES.append("{IP Address}:{Port}".format(**values))

    PROXIES_TIMESTAMP = time.time()

    if not PROXIES:
        if proxy_level == 'elite proxy':
            refresh_proxies(proxy_level='anonymous')
        else:
            raise NoProxysError


def choose_random_proxy():

    if not PROXIES:
        refresh_proxies()

    if PROXIES_TIMESTAMP - time.time() > 1800:
        refresh_proxies()

    return random.choice(PROXIES)


def requests_get(url, attempts=2):

    proxy_dict = {
        'https': 'https://{0}'.format(choose_random_proxy())
    }

    response = EMPTY_HTML

    try:
        response = requests.get(url, proxies=proxy_dict).text

    except (requests.exceptions.ProxyError, requests.exceptions.ConnectionError):
        if attempts:
            return requests_get(url, attempts=attempts-1)


    return response.strip()


def selenium_get(url, attempts=2):

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument(
        "--proxy-server={0}".format(choose_random_proxy())
    )
    try:
        driver = webdriver.Chrome(CHROMEDRIVER, chrome_options=options)
    except common.exceptions.SessionNotCreatedException:
        time.sleep(5)
        return selenium_get(url=url, attempts=attempts - 1)

    page_source = EMPTY_HTML

    try:
        driver.get(url)
        page_source = driver.page_source.strip()

    except (
        common.exceptions.TimeoutException,
        common.exceptions.SessionNotCreatedException
    ):

        if attempts:
            selenium_quit(driver)
            return selenium_get(url=url, attempts=attempts-1)

    return driver, page_source


def selenium_quit(driver):

    if driver:
        driver.quit()
        del driver
        gc.collect()


def load_page_source(
        url, load_via='requests',
        attempts=2, load_iframe=True
):

    base_url = url.split('/')[2]
    driver = None
    page_source = []

    if load_via == 'requests':

        response = requests_get(url)

        if response:
            page_source.append(response)

    elif load_via == 'selenium':

        driver, driver_source = selenium_get(url)

        page_source.append(driver_source)

        if load_iframe == True and base_url in IFRAMES:

            for method, selector in IFRAMES[base_url].items():

                try:
                    iframe = driver.find_element(method, selector)
                    driver.switch_to.frame(iframe)
                    page_source.append(driver.page_source.strip())

                except (common.exceptions.NoSuchElementException, common.exceptions.TimeoutException):
                    if attempts and load_iframe == True:
                        selenium_quit(driver)
                        return load_page_source(
                            url=url,
                            load_via=load_via,
                            attempts=attempts-1
                        )

    selenium_quit(driver)

    return page_source


def get_hyperlinks(url, load_via):

    hyperlinks = []

    base_url = url.split('/')[2]
    search_pattern = re.compile(WHITE_LIST[base_url])

    page_source = load_page_source(
        url,
        load_via=load_via,
        load_iframe=False
    )
    page_source = page_source[0] if page_source else EMPTY_HTML

    soup = BeautifulSoup(page_source, 'html.parser')
    anchor_tags = soup.findAll('a', href=search_pattern)

    for tag in anchor_tags:
        hyperlinks.append(tag.get('href'))

    hyperlinks = list(set(hyperlinks))

    return hyperlinks


def normalize_url(url, base_url):

    if base_url not in url and not url.startswith('http'):
        url = '{0}/{1}'.format(base_url, url)
        url = url.replace("//", "/")

        if url.startswith('http://'):
            url = url.replace('http://', 'https://')

        if not url.startswith('https://'):
            url = 'https://{0}'.format(url)

    return url.split("?")[0]


def save_page(hyperlink, seed_url, attempts=2):

    base_url = seed_url.split('/')[2]
    page_id = re.split(
        "[/-]+",
        re.search(WHITE_LIST[base_url], hyperlink).group()
    )[-1]
    file_name = r'{0}\{1}_{2}.json'.format(
        OUTPUT_PATH,
        ALIAS[base_url],
        page_id
    )

    if os.path.isfile(file_name):
        return False

    page_dict = dict(
        url=hyperlink,
        base_url=base_url,
        seed_url=seed_url,
        page_id=page_id,
        html=load_page_source(hyperlink, load_via=LOAD_VIA[base_url]),
        timestamp=time.strftime("%d.%m.%Y %H:%M:%S")
    )

    if not page_dict['html'][0].replace(EMPTY_HTML, ""):
        if attempts:
            save_page(hyperlink, base_url, attempts=attempts-1)

        else:
            raise HtmlLoadException(hyperlink)

    fobj = open(file_name, "w")
    fobj.write(json.dumps(page_dict, indent=4, sort_keys=True))
    fobj.close()

    if os.stat(file_name).st_size < 6200L:
        os.remove(file_name)
        raise HtmlLoadException(hyperlink)

    return True


def save_pages(hyperlinks):

    for hyperlink in hyperlinks:
        base_url = hyperlink.split('/')[2]
        try:
            save_page(hyperlink, base_url, attempts=2)
        except HtmlLoadException:
            print "unable to load {0}".format(hyperlink)


def crawler(seed_url):

    start = time.time()

    main_page_error = 0
    hyperlinks_error = 0
    saved_hyperlinks = 0
    reload_hyperlinks = []
    hyperlinks = []

    base_url = seed_url.split('/')[2]

    page = re.search(r'(page|seite)=(?P<page>\d{1,2})', seed_url)
    page = page.groupdict()['page'] if page else -1

    try:
        for method in ('requests', 'selenium'):
            hyperlinks = get_hyperlinks(seed_url, load_via=method)
            if hyperlinks:
                break

    except Exception:
        print traceback.print_exc()

        main_page_error += 1

    for hyperlink in hyperlinks:

        try:
            hyperlink = normalize_url(hyperlink, base_url)
            saved = save_page(hyperlink, seed_url)

            if saved:
                saved_hyperlinks += 1

        except HtmlLoadException:
            reload_hyperlinks.append(hyperlink)
            hyperlinks_error += 1

        except Exception:
            hyperlinks_error += 1

    stop = time.time()

    return main_page_error, hyperlinks_error, len(hyperlinks), reload_hyperlinks, base_url, page, stop-start, saved_hyperlinks


def frontier_manager(num_processes):

    reload_list = []

    seed_list = populate_seed_list()
    seed_list_len = len(seed_list)
    pool = multiprocessing.Pool(processes=num_processes)
    start = time.time()

    for count, result in enumerate(pool.imap_unordered(crawler, seed_list)):

        main_page_error, hyperlinks_error, found_hyperlinks, reload_hyperlinks, base_url, page, elapsed_time, saved_hyperlinks = result

        reload_list.append(reload_hyperlinks)

        print '{0:07.4f} %, ' \
              '{1:04d}/{2} seed pages done, ' \
              '{3:>20}, ' \
              'page no {4:>2}, ' \
              'total time {5}, ' \
              'process time {6}, ' \
              '{7:2d} hyperlinks found, ' \
              '{8:2d} hyperlinks saved, ' \
              '{9:2d} hyperlinks to reload, ' \
              '{10:2d} main page errors, ' \
              '{11:2d} hyperlinks errors'.format(
            count*100.0/seed_list_len,
            count,
            seed_list_len,
            base_url.replace("www.",""),
            page,
            convert_seconds(time.time() - start),
            convert_seconds(elapsed_time),
            found_hyperlinks,
            saved_hyperlinks,
            len(reload_hyperlinks),
            main_page_error,
            hyperlinks_error
        )

        #start = time.time()

    print "reloading {0} elements".format(len(reload_list))

    pool = multiprocessing.Pool(processes=num_processes)
    pool.map(save_pages, reload_list)


if __name__ == '__main__':
    #refresh_proxies()
    start_time = time.time()
    frontier_manager(60)
    stop_time = time.time()
    print "Done within {0} sec".format(stop_time-start_time)

