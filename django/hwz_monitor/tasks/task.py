import scrapy
from hwz_monitor.celery_app import app

from .hardwarezone.hardwarezone.spiders.spider import KiasuSpider
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerProcess

@app.task
def start_crawler(starting_url):
    process = CrawlerProcess(get_project_settings())

    process.crawl(KiasuSpider)
    process.start()
