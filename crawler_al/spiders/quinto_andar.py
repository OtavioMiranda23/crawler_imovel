import datetime
import scrapy


class QuintoAndarSpider(scrapy.Spider):
    name = "quinto_andar"
    start_urls = ["https://www.quintoandar.com.br/alugar/imovel/santa-cecilia-sao-paulo-sp-brasil/de-500-a-3000-reais/2-quartos"]
    def parse(self, response):
        for el in response.css("[data-testid='house-card-container-rent']"):
            yield {
                "endereco": el.css(".Cozy__CardContent-Container h2.CozyTypography::text")[-1].get(),
                "metragem": el.css(".Cozy__CardContent-Container h3.CozyTypography::text").get(),
                "valorTotal": el.css(".Cozy__CardTitle-Subtitle .CozyTypography::text").get(),
                "link": el.css("a::attr(href)").get(),
                "data_coleta": datetime.datetime.now().isoformat(),
            }
        pass
