# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from datetime import date
import json
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import sqlite3



class UniqueJSONPipeline:
    def __init__(self):
        self.items = []
        self.conexao = None
        self.cursor = None

    def open_spider(self, spider):
        self.conexao = sqlite3.connect("imoveis.db")
        self.cursor = self.conexao.cursor()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS imoveis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endereco TEXT NOT NULL,
            preco REAL NOT NULL,
            data_cadastro TEXT NOT NULL,
            disponivel BOOLEAN NOT NULL,
            site TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL UNIQUE,
            qtd_quartos INTEGER NOT NULL
        )
        """)

        self.conexao.commit()

    def process_item(self, item, spider):
        preco = item.get("valorTotal").replace("R$", "").replace(".", "").replace(",", ".").strip()
        self.cursor.execute("""
        INSERT INTO imoveis (endereco, preco, data_cadastro, disponivel, site, link, qtd_quartos)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (item.get("endereco"), preco, date.today().isoformat(), True, "Quinto Andar", item.get("link"), 2))
        return item
        
    def close_spider(self, spider):
        self.conexao.commit()
        self.conexao.close()