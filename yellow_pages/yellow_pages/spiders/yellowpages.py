# import scrapy
# from yellow_pages.items import YellowPagesItem
# from scrapy.http import JsonRequest
# from bs4 import BeautifulSoup as BS


# class YellowpagesSpider(scrapy.Spider):
#     name = "yellowpages"
#     allowed_domains = ["yellowpages.in"]

#     custom_settings = {
#         'DOWNLOAD_DELAY': 2,
#         'CONCURRENT_REQUESTS': 1,
#         'FEED_URI': 'output/quotes.json',
#         'FEED_FORMAT': 'json',
#         'FEED_EXPORT_ENCODING': 'utf-8',
#         'LOG_LEVEL': 'INFO'
#     }

#     start_urls = ["http://yellowpages.in/"]

#     def parse(self, response):
#         self.eind=50
#         for urls in response.css("ul#ulCats li"):
#             url = urls.css("a.eachHomeCategory::attr('href')").get()
#             self.uid = url.split("/")[-1]
#             self.refer = "http://yellowpages.in"+url
#             if url:
#                 yield response.follow(url, callback=self.parse_category)
#                 break

#     def parse_category(self, response):
#         # Follow all business listing links on the category page
#         for link in response.css(
#             "ul#MainContent_ulFList li div.popularTitleTextBlock a::attr('href')"
#         ):
#             href = link.get()
#             if href:
#                 yield response.follow(href, callback=self.parse_listing)
#                 # break

#         # If there is a load more button, handle pagination
#         if response.css("button.loadMoreBtn").get():
#             # yield the pagination request
#             yield from self.pagination()

#     def pagination(self,):

#         self.cookies = {
#             '_ga': 'GA1.2.1572373726.1757949287',
#             '_gid': 'GA1.2.215484978.1758625877',
#             'ASP.NET_SessionId': 'wmvmr5hajtbxtvsprukzumm2',
#             'AuthToken': '91589295-6195-45fa-9be2-7d05ba6cf625',
#             'ARRAffinity': '2c4bcfc49bcababadf604ef9ab0759f26a7284ede6d9af745ebfffe448c64a62',
#             '_gat': '1',
#         }

#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
#             'Accept': '*/*',
#             'Accept-Language': 'en-US,en;q=0.5',
#             # 'Accept-Encoding': 'gzip, deflate',
#             'Content-Type': 'application/json; charset=utf-8',
#             'Ticket': '91589295-6195-45fa-9be2-7d05ba6cf625',
#             'X-Requested-With': 'XMLHttpRequest',
#             'Origin': 'http://yellowpages.in',
#             'Connection': 'keep-alive',
#             'Referer': self.refer,
#             'Priority': 'u=0',
#         }

#         payload = {
#             'uId': self.uid,
#             'loc': 'hyderabad',
#             'eInd': self.eind,
#             'filterParams': [],
#             'sort': 'Popular',
#         }

#         yield JsonRequest(
#             url='http://yellowpages.in/helper.aspx/GetBusinessByCatFilter',
#             data=payload,
#             cookies=self.cookies,
#             headers=headers,
#             callback=self.parse_load
#         )
        


#     def parse_load(self, response):
#         # Inspect JSON data returned by the AJAX endpoint
#         soup = BS(response.json()["d"][0],"html.parser")
#         for i in soup.find_all("div","eachPopular"):
#             url = "http://yellowpages.in"+(i.find("div","popularTitleTextBlock").find("a").get("href"))
#             yield scrapy.Request(url, callback=self.parse_listing)
        
#         num1 = int(response.json()["d"][1])
#         num2 = int(response.json()["d"][2])
#         if num1 <= num2:
#             self.eind+=25
#             del self.cookies['_gat']
#             self.pagination()
#         else:
#             return

#     def parse_listing(self, response):
#         item = YellowPagesItem()
#         item['name'] = response.css('h1#MainContent_h1::text').get()
#         item['phone'] = response.css('a#MainContent_aTel::text').get()
#         item['address'] = ' '.join(
#             response.css('div.contactNumbers address::text').getall()
#         )
#         item['url'] = response.url

#         for i in response.css("ul#MainContent_ulTimings li"):
#             day = i.css('span.dayDisplay::text').get()
#             time_val = i.css('span.timeDisplay::text').get()
#             if day and time_val:
#                 item[day.lower()] = time_val

#         yield item



import json
import scrapy
from yellow_pages.items import YellowPagesItem
from scrapy.http import JsonRequest
from bs4 import BeautifulSoup as BS


class YellowpagesSpider(scrapy.Spider):
    name = "yellowpages"
    allowed_domains = ["yellowpages.in"]

    custom_settings = {
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        # 'FEED_URI': 'output/quotes.json',
        # 'FEED_FORMAT': 'json',
        # 'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'INFO'
    }

    start_urls = ["http://yellowpages.in/"]

    # Static defaults (you can move to settings or make dynamic)
    DEFAULT_COOKIES = {
        '_ga': 'GA1.2.1572373726.1757949287',
        '_gid': 'GA1.2.215484978.1758625877',
        'ASP.NET_SessionId': 'wmvmr5hajtbxtvsprukzumm2',
        'AuthToken': '91589295-6195-45fa-9be2-7d05ba6cf625',
        'ARRAffinity': '2c4bcfc49bcababadf604ef9ab0759f26a7284ede6d9af745ebfffe448c64a62',
        '_gat': '1',
    }

    PAGE_STEP = 25   # number of results per "page" (adjust if different)
    START_EIND = 50  # initial eInd as used in your original spider (adjust as needed)

    def parse(self, response):
        # find first category (original code used break after first)
        for urls in response.css("ul#ulCats li"):
            url = urls.css("a.eachHomeCategory::attr('href')").get()
            if url:
                # Build absolute referer and uid for this category and pass via meta
                uid = url.rstrip("/").split("/")[-1]
                refer = response.urljoin(url)
                # follow the category page and pass uid + refer
                yield response.follow(url, callback=self.parse_category, meta={'uid': uid, 'refer': refer})
                break

    def parse_category(self, response):
        uid = response.meta.get('uid')
        refer = response.meta.get('refer', response.url)

        # follow listing links on the category page
        for link in response.css("ul#MainContent_ulFList li div.popularTitleTextBlock a::attr('href')"):
            href = link.get()
            if href:
                yield response.follow(href, callback=self.parse_listing, meta={'source': 'category'})

        # if there is a load more button, start pagination with the initial eInd
        if response.css("button.loadMoreBtn").get():
            yield from self._make_pagination_request(uid=uid, refer=refer, eInd=self.START_EIND)

    def _make_pagination_request(self, uid, refer, eInd):
        """
        Build and yield a JsonRequest for pagination. We pass uid/refer/eInd in meta
        so parse_load has consistent request-specific state (no shared self.* mutation).
        """
        # copy cookies so future modifications don't affect other requests
        cookies = dict(self.DEFAULT_COOKIES)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Content-Type': 'application/json; charset=utf-8',
            'Ticket': '91589295-6195-45fa-9be2-7d05ba6cf625',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': 'http://yellowpages.in',
            'Connection': 'keep-alive',
            'Referer': refer,
            'Priority': 'u=0',
        }

        payload = {
            'uId': uid,
            'loc': 'hyderabad',
            'eInd': eInd,
            'filterParams': [],
            'sort': 'Popular',
        }

        self.logger.info("Requesting pagination: uid=%s eInd=%s", uid, eInd)

        yield JsonRequest(
            url='http://yellowpages.in/helper.aspx/GetBusinessByCatFilter',
            data=payload,
            cookies=cookies,
            headers=headers,
            callback=self.parse_load,
            meta={'uid': uid, 'refer': refer, 'eInd': eInd},
            dont_filter=True,
        )

    def parse_load(self, response):
        """
        Handle the JSON returned by pagination endpoint.
        This function:
          - parses HTML snippet returned in response.json()['d'][0]
          - yields Request for each listing found
          - decides whether to fetch the next page and yields that JsonRequest
        """
        meta = response.meta
        uid = meta.get('uid')
        refer = meta.get('refer')
        eInd = int(meta.get('eInd', self.START_EIND))

        # robust JSON handling
        try:
            data = response.json()
        except ValueError:
            self.logger.error("Invalid JSON response: %s", response.text[:200])
            return

        # Many ASP.NET endpoints wrap data in 'd' key: d[0] = HTML, d[1]=num1, d[2]=num2
        if isinstance(data, dict) and 'd' in data:
            d = data['d']
        else:
            d = data

        if not d or not isinstance(d, list) or len(d) < 1:
            self.logger.info("Empty/Unexpected pagination payload for uid=%s eInd=%s", uid, eInd)
            return

        # Parse HTML fragment (first element) and yield listing requests
        html_fragment = d[0]
        soup = BS(html_fragment, "html.parser")
        found_any = False
        for div in soup.find_all("div", class_="eachPopular"):
            # locate the inner link safely
            title_block = div.find("div", class_="popularTitleTextBlock")
            if not title_block:
                continue
            a = title_block.find("a", href=True)
            if not a:
                continue
            href = a.get("href")
            if not href:
                continue
            # make absolute URL and schedule parse_listing
            url = response.urljoin(href)
            found_any = True
            yield scrapy.Request(url, callback=self.parse_listing, meta={'source': 'pagination'})

        # Check pagination counters (if provided)
        # your original used d[1] and d[2]; guard against missing values
        try:
            num1 = int(d[1]) if len(d) > 1 else 0
            num2 = int(d[2]) if len(d) > 2 else 0
        except Exception:
            num1, num2 = 0, 0

        # Decide whether to request next page
        # original logic: if num1 <= num2 then continue; we'll keep that but yield next request
        if num1 <= num2:
            next_eInd = eInd + self.PAGE_STEP
            self.logger.info("Scheduling next page for uid=%s next_eInd=%s", uid, next_eInd)
            # yield the next JsonRequest (important: yield, not call)
            yield from self._make_pagination_request(uid=uid, refer=refer, eInd=next_eInd)
        else:
            self.logger.info("Pagination finished for uid=%s (eInd=%s)", uid, eInd)

    def parse_listing(self, response):
        item = YellowPagesItem()
        item['name'] = response.css('h1#MainContent_h1::text').get()
        item['phone'] = response.css('a#MainContent_aTel::text').get()
        item['address'] = ' '.join(
            response.css('div.contactNumbers address::text').getall()
        ).strip()
        item['url'] = response.url

        # initialize day fields to None or empty string
        for d in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
            item.setdefault(d, None)

        # mapping various scraped day representations to full-day keys
        day_key_map = {
            'mon': 'monday', 'monday': 'monday',
            'tue': 'tuesday', 'tues': 'tuesday', 'tuesday': 'tuesday',
            'wed': 'wednesday', 'weds': 'wednesday', 'wednesday': 'wednesday',
            'thu': 'thursday', 'thur': 'thursday', 'thurs': 'thursday', 'thursday': 'thursday',
            'fri': 'friday', 'friday': 'friday',
            'sat': 'saturday', 'saturday': 'saturday',
            'sun': 'sunday', 'sunday': 'sunday'
        }

        for i in response.css("ul#MainContent_ulTimings li"):
            raw_day = i.css('span.dayDisplay::text').get()
            time_val = i.css('span.timeDisplay::text').get()
            if not raw_day or not time_val:
                continue

            # normalize raw_day, keep only alphabetic portion, lowercase
            day_norm = ''.join(ch for ch in raw_day if ch.isalpha()).lower()
            key = day_key_map.get(day_norm)
            if key:
                item[key] = time_val.strip()
            else:
                # if unknown label, stash it in a generic field or log
                response.request.meta.setdefault('extra_days', []).append({raw_day: time_val.strip()})
                self.logger.debug("Unknown day label: %r -> %r", raw_day, time_val)

        yield item
