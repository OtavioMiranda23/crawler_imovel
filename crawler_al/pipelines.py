# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import os
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class UniqueJSONPipeline:
    def __init__(self):
        self.items = []
        self.existing_links = set()
        self.filename = "resultados.json"
        
    def open_spider(self, spider):
        if os.path.exists(self.filename):
            with open(self.filename, "r", encoding='utf-8') as f:
                try:
                    existing_data = json.load(f)
                    self.items = existing_data
                    self.existing_links = {item.get("link") for item in existing_data if "link" in item}
                except json.JSONDecodeError:
                    self.items = []
                    self.existing_links = set()

    def process_item(self, item, spider):
        unique_key = item.get("link")
        if unique_key and unique_key in self.existing_links:
            raise DropItem(f"Duplicate item found: {unique_key}")
        else:
            self.existing_links.add(unique_key)
            self.items.append(dict(item))
            return item
        
    def close_spider(self, spider):
        with open(self.filename, "w", encoding="utf-8") as f:
            json.dump(self.items, f, ensure_ascii=False, indent=2)