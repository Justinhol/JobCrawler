import requests
import json
from pathlib import Path
from datetime import datetime
import csv
import os


class ConferenceCrawler:
    def __init__(self):
        # 设置请求URL和参数
        self.base_url = "https://api2.openreview.net/notes"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
            "Accept": "application/json,text/*;q=0.99",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh-Hans;q=0.9",
            "Origin": "https://openreview.net",
            "Referer": "https://openreview.net/",
            "Cookie": "_ga=GA1.1.1549123682.1744185637; _ga_GTB25PBMVL=GS1.1.1744185636.1.1.1744187684.0.0.0; GCILB=\"0fa01f74bee504df\"",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site"
        }
        self.params = {
            "content.venue": "ICML 2024 Poster",
            "details": "replyCount,presentation,writable",
            "domain": "ICML.cc/2024/Conference",
            "limit": 1000,  # 设置为最大允许值
            "offset": 0
        }

    def fetch_conference_data_paginated(self):
        """使用分页方式获取所有会议数据"""
        all_notes = []  # 存储所有获取的notes
        offset = 0
        page_size = 1000  # 每页最大数据量

        print("开始分页获取数据...")

        while True:
            # 更新offset参数
            self.params["offset"] = offset

            # 发送请求获取当前页数据
            print(f"正在获取第 {offset//page_size + 1} 页数据 (offset={offset})...")
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=self.params
            )

            # 确保请求成功
            response.raise_for_status()

            # 解析JSON数据
            data = response.json()

            # 检查是否有notes数据
            if 'notes' not in data or not data['notes']:
                # 如果没有数据，说明已经获取完所有数据
                break

            # 获取本次返回的notes数量
            current_notes = data['notes']
            current_count = len(current_notes)
            all_notes.extend(current_notes)

            print(f"成功获取 {current_count} 条数据，当前总计: {len(all_notes)}")

            # 如果返回的数据量小于page_size，说明已经是最后一页
            if current_count < page_size:
                break

            # 更新offset，准备获取下一页
            offset += page_size

        print(f"数据获取完成，共 {len(all_notes)} 条记录")

        # 构建完整的返回数据结构
        result = {
            'notes': all_notes,
            # 保留原始数据中可能有的其他字段
            **{k: v for k, v in data.items() if k != 'notes'}
        }

        return result

    def save_data_to_json(self, data):
        """保存数据到JSON文件，确保原子性写入"""
        # 生成文件名（使用当前时间戳）
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ICML-2024-Oral-{current_time}.json"
        temp_filename = f"ICML-2024-Oral-{current_time}.tmp.json"

        # 先写入临时文件
        with open(temp_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()  # 确保写入完成
            os.fsync(f.fileno())  # 确保数据刷新到磁盘

        # 重命名为最终文件名，确保原子性
        Path(temp_filename).rename(filename)

        return filename

    def extract_data_to_csv(self, data):
        """从JSON数据中提取信息并保存为CSV文件"""
        # 检查数据是否包含notes字段
        if 'notes' not in data:
            print("数据中没有找到notes字段")
            return None

        # 获取当前时间用于文件名
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"ICML-2024-Oral-{current_time}.csv"
        temp_csv_filename = f"ICML-2024-Oral-{current_time}.tmp.csv"

        try:
            # 打开临时文件进行写入
            with open(temp_csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                # 定义CSV列头
                fieldnames = ['id', 'title', 'keywords', 'tldr', 'abstract']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                # 遍历所有论文数据
                for note in data['notes']:
                    row = {'id': note.get('id', '')}

                    # 提取content中的内容，处理可能存在的嵌套结构
                    content = note.get('content', {})

                    # 提取标题
                    if 'title' in content and isinstance(content['title'], dict) and 'value' in content['title']:
                        row['title'] = content['title']['value']
                    else:
                        row['title'] = ''

                    # 提取关键词，可能有多个，用分号连接
                    if 'keywords' in content:
                        if isinstance(content['keywords'], dict) and 'value' in content['keywords']:
                            # 如果是字典格式，提取value值
                            keywords_value = content['keywords']['value']
                            if isinstance(keywords_value, list):
                                row['keywords'] = '; '.join(keywords_value)
                            else:
                                row['keywords'] = str(keywords_value)
                        elif isinstance(content['keywords'], list):
                            # 如果直接是列表，连接所有值
                            keywords_list = []
                            for kw in content['keywords']:
                                if isinstance(kw, dict) and 'value' in kw:
                                    keywords_list.append(kw['value'])
                                else:
                                    keywords_list.append(str(kw))
                            row['keywords'] = '; '.join(keywords_list)
                        else:
                            row['keywords'] = str(content['keywords'])
                    else:
                        row['keywords'] = ''

                    # 提取TLDR
                    if 'TLDR' in content and isinstance(content['TLDR'], dict) and 'value' in content['TLDR']:
                        row['tldr'] = content['TLDR']['value']
                    else:
                        row['tldr'] = ''

                    # 提取摘要
                    if 'abstract' in content and isinstance(content['abstract'], dict) and 'value' in content['abstract']:
                        row['abstract'] = content['abstract']['value']
                    else:
                        row['abstract'] = ''

                    # 写入一行数据
                    writer.writerow(row)

                # 确保数据写入磁盘
                csvfile.flush()
                os.fsync(csvfile.fileno())

            # 重命名临时文件为最终文件名，确保原子性
            Path(temp_csv_filename).rename(csv_filename)
            return csv_filename

        except Exception as e:
            # 发生错误时清理临时文件
            if Path(temp_csv_filename).exists():
                Path(temp_csv_filename).unlink()
            print(f"提取数据到CSV时发生错误：{str(e)}")
            return None


if __name__ == "__main__":
    # 创建爬虫实例
    crawler = ConferenceCrawler()

    try:
        # 使用分页方式获取所有数据
        conference_data = crawler.fetch_conference_data_paginated()

        # 保存原始JSON数据
        saved_json = crawler.save_data_to_json(conference_data)
        print(f"JSON数据已保存到：{saved_json}")

        # 提取数据并保存为CSV
        saved_csv = crawler.extract_data_to_csv(conference_data)
        if saved_csv:
            print(f"CSV数据已保存到：{saved_csv}")
        else:
            print("CSV数据保存失败")

    except Exception as e:
        print(f"发生错误：{str(e)}")
