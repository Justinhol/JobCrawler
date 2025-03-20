import requests
import json
import time
import math
from typing import Dict, Any, List
from jobsdb_job_details import get_job_details
import random
import os
from datetime import datetime
import csv
import pandas as pd
import threading
from queue import Queue
import concurrent.futures
import uuid

total_pages = 0
pageSize = 100  # 最大只能100


class CrawlerConfig:
    """双进度配置"""

    def __init__(
        self,
        num_threads=6,            # 并发线程数
        user_switch_freq=1000,       # 用户切换频率
        page_progress_step=10,     # 页面进度打印间隔
        job_progress_step=500       # 职位进度打印间隔
    ):
        self.num_threads = num_threads
        self.user_switch_freq = user_switch_freq
        self.page_progress_step = page_progress_step
        self.job_progress_step = job_progress_step
        self.validate()

    def validate(self):
        if self.page_progress_step < 1 or self.job_progress_step < 1:
            raise ValueError("进度步长必须≥1")


class JobsDBCrawler:
    def __init__(self, config=CrawlerConfig()):
        self.config = config
        # 使用配置参数
        self.base_url = "https://hk.jobsdb.com/api/jobsearch/v5/search"
        self.request_count = 0
        self.current_user_index = 0
        self.headers = {
            "User-Agent": self._generate_user_agent()
        }
        self.params = {
            "siteKey": "HK-Main",
            "sourcesystem": "houston",
            "classification": "6123,6281,1200,6251,6304,1203,1204,1225,6246,6261,1223,6362,6043,1220,6058,6008,6092,1216,1214,6317,1212,1211,1210,6205,1209,6263,6076,1206,6163,7019",
            "sortmode": "ListedDate",
            "pageSize": pageSize,
            "include": "seodata,relatedsearches,joracrosslink,gptTargeting",
            "locale": "en-HK",
        }

    def _generate_cookie(self):
        """动态生成cookie"""
        fbp = f"fb.1.{int(time.time()*1000)}.{random.randint(10**17, 10**18-1)}"
        dd_s = f"rum=0&expire={int(time.time())+3600}&logs=0"
        sol_id = f"{uuid.uuid4().hex[:16]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}"
        return f'_fbp={fbp}; _dd_s={dd_s}; sol_id={sol_id}'

    def _generate_session_id(self):
        """生成符合UUID格式的session ID"""
        return str(uuid.uuid4())

    def _generate_user_agent(self):
        """动态生成Safari User-Agent"""
        major = random.randint(15, 27)
        version = f"{random.randint(10,20)}{chr(ord('A')+random.randint(0,5))}"
        os_version = f"{random.randint(10,20)}_{random.randint(0,5)}"
        return f"Mozilla/5.0 (Macintosh; Intel Mac OS X {os_version}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{major}.{random.randint(0,3)} Safari/605.1.15"

    def get_user_info(self):
        """动态生成用户信息"""
        # 使用配置参数替换硬编码
        if self.request_count >= self.config.user_switch_freq:
            self.current_user_index += 1
            self.request_count = 0
            print(f"信息: 生成新用户配置 {self.current_user_index + 1}")
            self.headers["User-Agent"] = self._generate_user_agent()

        self.request_count += 1
        return {
            'cookie': self._generate_cookie(),
            'session_id': self._generate_session_id(),
            'user_agent': self.headers["User-Agent"]
        }

    def fetch_page(self, page: int) -> Dict[str, Any]:
        """获取指定页码的数据"""
        self.params["page"] = page
        try:
            response = requests.get(
                self.base_url,
                headers=self.headers,
                params=self.params
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"错误: 获取第 {page} 页时失败: {str(e)}")
            return {}

    def crawl_all_pages(self, total_pages):
        """爬取所有页面的数据"""
        all_ids = []
        data = self.fetch_page(1)
        total_jobs = data["solMetadata"]["totalJobCount"]
        total_pages = math.ceil(total_jobs / pageSize)
        print(f"开始爬取，共 {total_jobs} 个职位，{total_pages} 页")
        time.sleep(random.uniform(1.5, 2.5))

        for page in range(1, total_pages + 1):
            if page % self.config.page_progress_step == 0:
                print(f"页面进度: {page}/{total_pages}")

            data = self.fetch_page(page)
            if data:
                for jobs in data["data"]:
                    all_ids.append(jobs["solMetadata"]["jobId"])
            else:
                print(f"警告: 第 {page} 页数据获取失败")

            time.sleep(random.uniform(1.5, 2.5))

        # 反转ID列表，使最早发布的职位在前
        all_ids.reverse()

        # 修改分块逻辑为使用动态线程数
        job_chunks = [[] for _ in range(self.config.num_threads)]
        for idx, job_id in enumerate(all_ids):
            thread_idx = idx % self.config.num_threads
            job_chunks[thread_idx].append(job_id)

        # 获取当前时间并格式化为 YYYYMMDDHHMMSS 格式
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")

        # 先创建临时文件夹（使用初始数量）
        initial_count = len(all_ids)
        folder_name = f"{current_time}_temp_{initial_count}"
        try:
            os.makedirs(folder_name)
        except FileExistsError:
            print(f"警告: 文件夹 '{folder_name}' 已存在")

        print(f"\n开始处理职位详情，共 {len(all_ids)} 个职位")

        # 创建并启动线程
        threads = []
        thread_results = []

        for i in range(self.config.num_threads):
            thread = ThreadedCrawler(job_chunks[i], i, folder_name, self)
            threads.append(thread)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.num_threads) as executor:
            future_to_thread = {executor.submit(
                thread.process_jobs): thread for thread in threads}
            for future in concurrent.futures.as_completed(future_to_thread):
                thread = future_to_thread[future]
                try:
                    success_count, failed_ids = future.result()
                    thread_results.append((success_count, failed_ids))
                except Exception as e:
                    print(f"线程 {thread.thread_id} 执行失败: {str(e)}")

        # 统计结果
        total_success = sum(result[0] for result in thread_results)
        all_failed_ids = [id for result in thread_results for id in result[1]]

        # 重命名文件夹（使用实际成功数量）
        new_folder_name = f"{current_time}_{total_success}"
        try:
            os.rename(folder_name, new_folder_name)
            print(f"\n文件夹已重命名为: {new_folder_name}")
        except Exception as e:
            print(f"文件夹重命名失败: {str(e)}，保持原名称: {folder_name}")
            new_folder_name = folder_name

        # 合并CSV文件（使用新文件夹名称）
        print("\n合并CSV文件...")
        combined_csv_path = f'./{new_folder_name}/{new_folder_name}.csv'
        df_list = []

        # 修改线程池工作线程数
        for i in range(self.config.num_threads):
            thread_csv = f'./{new_folder_name}/thread_{i}.csv'
            if os.path.exists(thread_csv):
                df = pd.read_csv(thread_csv, encoding='utf-8')
                df_list.append(df)
                os.remove(thread_csv)

        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            combined_df.to_csv(combined_csv_path,
                               index=False, encoding='utf-8')

        # 在统计结果后添加验证代码
        json_count = len(os.listdir(f'./{new_folder_name}/original_data'))
        print(f"JSON文件实际存储数量: {json_count}")
        print(
            f"成功计数与文件数一致性检查: {'通过' if json_count == total_success else '异常'}")

        print(f"\n任务完成!")
        print(f"总数: {len(all_ids)}")
        print(f"成功: {total_success}")
        if all_failed_ids:
            print(f"失败: {len(all_failed_ids)}")
            with open(f'./{new_folder_name}/failed_ids.json', 'w', encoding='utf-8') as f:
                json.dump(all_failed_ids, f, ensure_ascii=False, indent=2)
            print(f"失败的ID已保存到 {new_folder_name}/failed_ids.json")


class ThreadedCrawler:
    def __init__(self, job_ids: List[str], thread_id: int, folder_name: str, crawler_instance):
        self.job_ids = job_ids
        self.thread_id = thread_id
        self.folder_name = folder_name
        self.crawler_instance = crawler_instance
        self.thread_request_count = 0
        self._init_user_config()  # 初始化配置

    def _init_user_config(self):
        """初始化线程配置"""
        self.thread_headers = {
            "User-Agent": self._generate_thread_user_agent()
        }
        self.current_cookie = self._generate_cookie()
        self.current_session = self._generate_session_id()

    def _generate_thread_user_agent(self):
        """生成线程专属User-Agent"""
        major = random.randint(15, 27)
        version = f"{random.randint(10,20)}{chr(ord('A')+random.randint(0,5))}"
        os_version = f"{random.randint(10,20)}_{random.randint(0,5)}"
        return f"Mozilla/5.0 (Macintosh; Intel Mac OS X {os_version}) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/{major}.{random.randint(0,3)} Safari/605.1.15"

    def _generate_cookie(self):
        """动态生成cookie"""
        fbp = f"fb.1.{int(time.time()*1000)}.{random.randint(10**17, 10**18-1)}"
        dd_s = f"rum=0&expire={int(time.time())+3600}&logs=0"
        sol_id = f"{uuid.uuid4().hex[:16]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}-{uuid.uuid4().hex[:12]}"
        return f'_fbp={fbp}; _dd_s={dd_s}; sol_id={sol_id}'

    def _generate_session_id(self):
        """生成符合UUID格式的session ID"""
        return str(uuid.uuid4())

    def get_thread_user_info(self):
        """获取当前配置"""
        return {
            'cookie': self.current_cookie,
            'session_id': self.current_session,
            'user_agent': self.thread_headers["User-Agent"]
        }

    def _rotate_user_config(self):
        """统一更换配置"""
        print(f"线程 {self.thread_id} 更换配置")
        self._init_user_config()  # 重新生成所有配置
        self.thread_request_count = 0

    def process_jobs(self):
        csv_file = f'./{self.folder_name}/thread_{self.thread_id}.csv'
        success_count = 0
        failed_ids = []
        start_time = time.time()  # 记录全局时间基准

        for idx, job_id in enumerate(self.job_ids):
            # 计算每个请求的预定发送时间
            scheduled_time = start_time + (
                idx * 1.5 +  # 固定请求间隔
                self.thread_id * 0.5  # 线程间延迟
            )
            now = time.time()
            delay = scheduled_time - now
            if delay > 0:
                time.sleep(delay)

            try:
                data = get_job_details(job_id, self)
                if data and 'data' in data and 'jobDetails' in data['data'] and 'job' in data['data']['jobDetails']:
                    # 创建original_data目录（如果不存在）
                    original_data_folder = f'./{self.folder_name}/original_data'
                    os.makedirs(original_data_folder, exist_ok=True)

                    # 写入JSON文件
                    with open(f'{original_data_folder}/{job_id}.json', 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False,
                                  indent=2)  # 只有数据有效时才会写入

                    # 只有成功写入JSON后才会计入成功计数
                    self.extract_to_csv(csv_file, job_id, data)
                    success_count += 1

                    # 每500个职位打印一次进度
                    if success_count % self.crawler_instance.config.job_progress_step == 0:
                        print(
                            f"线程 {self.thread_id} 职位进度: {success_count}/{len(self.job_ids)}")
                else:
                    failed_ids.append(job_id)

            except Exception as e:
                print(f"错误: 线程 {self.thread_id} 处理 ID {job_id} 时失败: {str(e)}")
                failed_ids.append(job_id)

            # 每次请求后增加计数并检查是否需要更换配置
            self.thread_request_count += 1
            if self.thread_request_count >= self.crawler_instance.config.user_switch_freq:
                self._rotate_user_config()

        return success_count, failed_ids

    def extract_to_csv(self, csv_file, job_id, data):
        try:
            job_data = data['data']['jobDetails']['job']

            # 获取products数据，增加空值处理
            products = job_data.get('products', {}) or {}
            bullets = products.get('bullets', []) if products else []
            questionnaire = products.get(
                'questionnaire', {}) if products else {}
            questions = questionnaire.get(
                'questions', []) if questionnaire else []

            # 提取所需数据
            extracted_data = {
                'job_id': job_id,
                'classifications': job_data.get('classifications', [{}])[0].get('label', '') if job_data.get('classifications') else '',
                'title': job_data.get('title', ''),
                'post_time': job_data.get('listedAt', {}).get('dateTimeUtc', '') if job_data.get('listedAt') else '',
                'expires_time': job_data.get('expiresAt', {}).get('dateTimeUtc', '') if job_data.get('expiresAt') else '',
                'abstract': job_data.get('abstract', ''),
                'content': job_data.get('content', ''),
                'salary': job_data.get('salary', {}).get('label', '') if job_data.get('salary') else '',
                'link': job_data.get('shareLink', ''),
                'work_types': job_data.get('workTypes', {}).get('label', '') if job_data.get('workTypes') else '',
                'advertiser': job_data.get('advertiser', {}).get('name', '') if job_data.get('advertiser') else '',
                'location': job_data.get('location', {}).get('label', '') if job_data.get('location') else '',
                'bullets': ','.join(bullets) if bullets else '',
                'questions': ','.join(questions) if questions else ''
            }

            # 检查CSV文件是否存在
            file_exists = os.path.isfile(csv_file)

            # 写入CSV文件
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=extracted_data.keys())
                if not file_exists:
                    writer.writeheader()
                writer.writerow(extracted_data)
        except Exception as e:
            print(f"错误: 线程 {self.thread_id} 提取ID {job_id} 数据到CSV时失败: {str(e)}")
            raise

    def record_failure(self):
        """记录失败并更换配置（如果达到阈值）"""
        self.thread_request_count += 1
        if self.thread_request_count >= self.crawler_instance.config.user_switch_freq:
            self._rotate_user_config()


if __name__ == "__main__":
    # 使用默认配置
    config = CrawlerConfig()  # ✔️ 参数默认值生效
    # 自定义配置示例（已注释）
    # config = CrawlerConfig(num_threads=12)  # ✔️ 可自定义
    crawler = JobsDBCrawler(config)  # ✔️ 配置传入
    crawler.crawl_all_pages(total_pages)
