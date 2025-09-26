import scrapy
from scrapy.http import JsonRequest
from scrapy.crawler import CrawlerProcess


class MySpider(scrapy.Spider):
    name = "myspider"
    search_term = input("Enter search term: ")
    start_urls = [f"https://www.flipkart.com/search?q={search_term}"]

    def parse(self, response):
        print("Response URL:", response.url)  # Print the URL of the response
        print("Response Status:", response.status)  # Print the HTTP status code of the response

        for i in response.css("div.tUxRFH a.CGtC98::attr('href')"):
            yield response.follow(i.get(), callback=self.parse_product)

        # Handle pagination
        if response.css("nav.WSL9JP a._9QVEpD span::text")[-1].get() == "Next":
            next_page = response.css("nav.WSL9JP a._9QVEpD::attr('href')")[-1].get()
            if next_page:
                yield response.follow(next_page, callback=self.parse)

    def parse_product(self, response):
        try:
            name = response.css("h1._6EBuvT span.VU-ZEz::text").get()
        except Exception:
            name = None

        try:
            rating = response.css("div.ISksQ2 div.XQDdHH::text").get()
        except Exception:
            rating = None

        try:
            price = response.css("div.UOCQB1 div.hl05eU div.Nx9bqj::text").get()
        except Exception:
            price = None

        try:
            color_availability = ", ".join(
                response.xpath('(//ul[@class="hSEbzK"])[1]//text()').getall()
            )
        except Exception:
            color_availability = None

        data = {
            "Product Name": name,
            "Rating": rating,
            "Price": price,
            "Color Availability": color_availability,
        }

        yield data


process = CrawlerProcess(
    settings={
        'LOG_LEVEL': 'INFO',
        'FEED_URI': 'output/products.json',
        'FEED_FORMAT': 'json',
        'overwrite': True,
        'FEED_EXPORT_ENCODING': 'utf-8',
    }
)  # Add your desired settings here
process.crawl(MySpider)  # Add your spider class here
process.start()  # the script will block here until the crawling is finished





