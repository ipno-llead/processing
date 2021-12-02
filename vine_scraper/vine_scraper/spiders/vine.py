import sys
sys.path.append('../')
from lib.path import data_file_path
from scrapy.spiders import Spider
from scrapy.http import FormRequest


class MySpider(Spider):
    name = 'vine_scraper'

    def start_requests(self):
        with open(data_file_path('raw/vine/vine_urls.csv')) as f:
            for line in f:
                if not line.strip():
                    continue
                yield FormRequest(
                    url=line,
                    method='GET',
                    callback=self.parse,
                    formdata={'__EVENTTARGET': 'lbShowAll'})

    def parse(self, response):
        for tr in response.css('table#gvRoster tr')[1:]:
            yield {
                'url': response.url,
                'name': tr.css('td:nth-child(2)::text')[0].get(),
                'race': tr.css('td:nth-child(4)::text')[0].get(),
                'sex': tr.css('td:nth-child(5)::text')[0].get(),
                'date_of_arrest': tr.css('td:nth-child(6)::text')[0].get()}
