# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class YellowPagesItem(scrapy.Item):
    # define the fields for your item here like:
    name = scrapy.Field()
    phone = scrapy.Field()
    address = scrapy.Field()
    url = scrapy.Field()
    sunday = scrapy.Field()
    monday = scrapy.Field()
    tuesday = scrapy.Field()
    wednesday = scrapy.Field()
    thursday = scrapy.Field()
    friday = scrapy.Field()
    saturday = scrapy.Field()   
    pass
