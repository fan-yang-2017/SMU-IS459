import scrapy


class KiasuSpider(scrapy.Spider):
    name = 'hardwarezone'

    start_urls = [
        'https://forums.hardwarezone.com.sg/forums/pc-gaming.382/',
    ]

    def parse(self, response):
        topic = response.xpath('//h1[@class="p-title-value"]/text()').get()

        for post in response.xpath('//article[starts-with(@data-content,"post")]'):
            contentlist = post.xpath('div/div/div/div/div/article/div[@class="bbWrapper"]/text()').getall()
            post_content = ' '.join(contentlist)
            post_content = post_content.replace('\n', '')
            post_content = post_content.replace('\t', '')
            post_content = post_content.replace('\u', '')

            yield {
                'topic': topic,
                'author': post.xpath('div/div/section/div/h4/a/text()').get(),
                'content': post_content,
            }

        for topic in response.xpath('//div[@class="structItem-title"]'):
            topic_link = topic.xpath('a/@href').get()
            if topic_link is not None:
                yield response.follow(topic_link, self.parse)

        next_page = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]/@href').get()
        if next_page is not None:
            yield response.follow(next_page, self.parse)
