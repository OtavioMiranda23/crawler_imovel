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
            link TEXT NOT NULL UNIQUE,
            qtd_quartos INTEGER NOT NULL
        )
        """)

        self.conexao.commit()
        #tornar todos os registros do banco como inativos
        self.cursor.execute(" UPDATE imoveis SET disponivel = 0 WHERE disponivel = 1")
        self.conexao.commit()
        

    def process_item(self, item, spider):
        preco = item.get("valorTotal").replace("total", "").replace("R$", "").replace(".", "").replace(",", ".").strip()
        #se o link do novo item não exisitr no banco, então criar um novo item
        self.cursor.execute("SELECT * FROM imoveis WHERE link = ?", (item.get("link"),))
        imovel = self.cursor.fetchone()
        if imovel is None:
            self.cursor.execute("""
            INSERT INTO imoveis (endereco, preco, data_cadastro, disponivel, site, link, qtd_quartos)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (item.get("endereco"), preco, date.today().isoformat(), 1, "Quinto Andar", item.get("link"), 2))
        else:
            #se o link existir, tornar-lo true
            self.cursor.execute("""
                UPDATE imoveis SET disponivel = ? WHERE link = ?
            """, (True, item.get("link")))
        return item
        
    def close_spider(self, spider):
        self.conexao.commit()
        self.conexao.close()