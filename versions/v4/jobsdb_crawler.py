import requests
import json
import time
import math
from typing import Dict, Any, List
from jobsdb_job_details import get_job_details, safe_get
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
            # "classification": "1223",
            "sortmode": "ListedDate",
            "pageSize": pageSize,
            "include": "seodata,relatedsearches,joracrosslink,gptTargeting",
            "locale": "en-HK",
        }
        self.total_processed = 0
        self.lock = threading.Lock()
        self.total_jobs = 0
        self.all_ids = []  # 将all_ids改为实例变量

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

    def fetch_page(self, page: int, max_retries=3) -> Dict[str, Any]:
        """获取指定页码的数据（添加重试机制）"""
        self.params["page"] = page
        for attempt in range(max_retries):
            try:
                # 每次重试前更新用户配置
                if attempt > 0:
                    self.get_user_info()
                    print(f"第{page}页重试中（第{attempt}次）...")

                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=self.params
                )
                response.raise_for_status()

                # 添加成功打印
                if attempt > 0:
                    print(f"第{page}页第{attempt}次重试成功")
                return response.json()

            except requests.RequestException as e:
                print(f"错误: 获取第 {page} 页时失败: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(3)  # 失败后等待3秒

        # 所有重试失败后打印
        print(f"第{page}页所有{max_retries}次重试均失败")
        return {}

    def crawl_all_pages(self, total_pages):
        """爬取所有页面的数据"""
        # 移除 all_ids 局部变量，使用类的实例变量 self.all_ids
        # 爬取第一页获取职位总数
        data = self.fetch_page(1)
        self.total_jobs = data["solMetadata"]["totalJobCount"]
        # 计算总页数
        total_pages = math.ceil(self.total_jobs / pageSize)
        print(f"开始爬取，共 {self.total_jobs} 个职位，{total_pages} 页")
        time.sleep(1)

        # 保持data目录下的路径结构
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        folder_name = f"data/{current_time}_temp"  # 路径仍保留在data目录
        os.makedirs(folder_name, exist_ok=True)  # 仅创建时间戳目录

        # 第一阶段：获取所有ID
        for page in range(1, total_pages + 1):
            if page % self.config.page_progress_step == 0:
                print(f"页面进度: {page}/{total_pages}")
            data = self.fetch_page(page)
            if data:
                for jobs in data["data"]:
                    self.all_ids.append(
                        jobs["solMetadata"]["jobId"])  # 使用self.all_ids
            time.sleep(1)

        # 反转ID列表，使最早发布的职位在前
        len_before = len(self.all_ids)
        self.all_ids = list(set(self.all_ids))  # 去重
        self.all_ids.reverse()
        print(
            f"\n开始抓取 {len(self.all_ids)} 个职位，已去重 {len_before - len(self.all_ids)} 个")

        # 处理阶段
        total_success, all_failed_ids = self.process_jobs_with_retry(
            self.all_ids,
            folder_name,
            max_retries=1
        )

        # 合并各线程临时文件夹到original_data（带完整路径检查）
        original_data_folder = os.path.join(folder_name, 'original_data')
        os.makedirs(original_data_folder, exist_ok=True)
        print(f"合并临时文件到: {original_data_folder}")

        for thread_id in range(self.config.num_threads):
            thread_temp_folder = os.path.join(
                folder_name, f'thread_{thread_id}_temp')
            if not os.path.exists(thread_temp_folder):
                continue

            print(f"处理线程{thread_id}的临时文件...")
            for filename in os.listdir(thread_temp_folder):
                src = os.path.join(thread_temp_folder, filename)
                if not os.path.isfile(src):
                    continue

                dst = os.path.join(original_data_folder, filename)
                # 处理文件名冲突
                counter = 1
                while os.path.exists(dst):
                    base, ext = os.path.splitext(filename)
                    dst = os.path.join(original_data_folder,
                                       f"{base}_{thread_id}_{counter}{ext}")
                    counter += 1

                try:
                    os.rename(src, dst)
                except Exception as e:
                    print(f"移动文件失败: {str(e)}")
                    continue

            # 删除空文件夹
            try:
                os.rmdir(thread_temp_folder)
            except OSError:
                print(f"警告: {thread_temp_folder} 非空，无法删除")

        if all_failed_ids:
            print(f"\n开始重试失败的 {len(all_failed_ids)} 个职位")
            retry_success, final_failed_ids = self.process_jobs_with_retry(
                all_failed_ids,
                folder_name,
                max_retries=1,
                is_retry=True
            )
            total_success += retry_success
            all_failed_ids = final_failed_ids
            print(f"重试结果: 成功 {retry_success} 个，仍失败 {len(final_failed_ids)} 个")

        # 统一提取CSV
        print("\n开始提取所有JSON到CSV...")
        csv_success, csv_failed = self.extract_all_json_to_csv(folder_name)
        total_csv_success = csv_success

        # CSV提取重试
        if csv_failed:
            print(f"\n开始重试失败的 {len(csv_failed)} 个CSV提取")
            retry_success, still_failed = self.extract_all_json_to_csv(
                folder_name, csv_failed)
            total_csv_success += retry_success
            print(f"CSV重试结果: 成功 {retry_success} 个，仍失败 {len(still_failed)} 个")
            if still_failed:
                with open(f'./{folder_name}/csv_failed.json', 'w') as f:
                    json.dump(still_failed, f)

        # 重命名文件夹（使用实际成功数量）
        # 路径包含data目录
        new_folder_name = f"data/{current_time}_{total_csv_success}"
        os.rename(folder_name, new_folder_name)

        # 合并CSV文件
        print("\n合并CSV文件...")
        self.merge_csv_files(new_folder_name)

        # 最终统计
        # 添加实际文件计数来验证统计准确性
        original_data_folder = os.path.join(new_folder_name, 'original_data')
        actual_json_count = sum(1 for f in os.listdir(
            original_data_folder) if f.endswith('.json'))

        print(f"\n任务完成!")
        print(f"总数: {len(self.all_ids)}")
        print(f"JSON存储成功: {total_success}")
        print(f"实际JSON文件数: {actual_json_count}")  # 添加实际文件数统计
        print(f"CSV提取成功: {total_csv_success}")
        print(f"最终失败: {len(all_failed_ids)}")
        # 添加差异统计
        print(f"JSON文件与CSV提取差异: {actual_json_count - total_csv_success}")
        print(
            f"CSV提取失败: {len(still_failed) if 'still_failed' in locals() else 0}")

    def process_jobs_with_retry(self, job_ids, folder_name, max_retries=1, is_retry=False):
        """通用处理逻辑（支持重试）"""
        total_success = 0
        all_failed_ids = []

        for attempt in range(max_retries):
            print(f"\n{'重试' if is_retry else '处理'}阶段（第{attempt+1}次尝试）")

            # 新增：重试阶段使用单线程
            if is_retry:
                # 主线程直接处理所有失败ID
                thread = ThreadedCrawler(job_ids, 0, folder_name, self)
                success_count, failed_ids = thread.process_jobs()
                total_success += success_count
                all_failed_ids = failed_ids
                break  # 重试阶段不循环
            else:
                # 原有分块多线程逻辑保持不变
                # 防御性去重确保数据唯一性
                unique_job_ids = list(set(job_ids))
                job_chunks = [[] for _ in range(self.config.num_threads)]
                for idx, job_id in enumerate(unique_job_ids):
                    thread_idx = idx % self.config.num_threads
                    job_chunks[thread_idx].append(job_id)

                threads = []
                thread_results = []
                for i in range(self.config.num_threads):
                    thread = ThreadedCrawler(
                        job_chunks[i], i, folder_name, self)
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

                # 统计本轮结果
                current_success = sum(result[0] for result in thread_results)
                current_failed = [
                    id for result in thread_results for id in result[1]]

                total_success += current_success
                all_failed_ids += current_failed  # 累积所有失败ID（已修正）

                if not current_failed:
                    break

        return total_success, all_failed_ids

    def merge_csv_files(self, folder_name):
        """合并CSV文件（现在只需重命名）"""
        source = f'./{folder_name}/all_jobs.csv'
        target = f'./{folder_name}/{folder_name.split("/")[-1]}.csv'  # 保持文件名正确
        if os.path.exists(source):
            os.rename(source, target)
            print(f"CSV文件已合并到: {target}")

    def extract_all_json_to_csv(self, folder_name, failed_files=None):
        """从JSON文件统一提取CSV"""
        csv_path = f'./{folder_name}/all_jobs.csv'
        json_folder = f'./{folder_name}/original_data'  # 路径已自动包含data目录
        success_count = 0
        local_failed = []

        # 获取实际JSON文件数量
        actual_json_count = sum(1 for f in os.listdir(
            json_folder) if f.endswith('.json'))

        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = None
            # 根据是否重试决定处理文件列表
            files_to_process = [
                f"{jid}.json" for jid in failed_files] if failed_files else os.listdir(json_folder)

            # 打印实际处理文件数量信息
            print(
                f"实际JSON文件数量: {actual_json_count}, 即将处理文件数量: {len([f for f in files_to_process if f.endswith('.json')])}")

            for filename in files_to_process:
                if not filename.endswith('.json'):
                    continue

                try:
                    with open(f'{json_folder}/{filename}', 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        extracted = self.extract_single_job(
                            data, filename.split('.')[0])
                        if not writer:
                            writer = csv.DictWriter(
                                csvfile, fieldnames=extracted.keys())
                            writer.writeheader()
                        writer.writerow(extracted)
                        success_count += 1
                except Exception as e:
                    print(f"提取 {filename} 失败: {str(e)}")
                    local_failed.append(filename.split('.')[
                                        0])  # 保存不带扩展名的job_id

        return success_count, local_failed

    def extract_single_job(self, data, job_id):
        """单个JSON数据提取"""
        job_data = data.get('data', {}).get('jobDetails', {}).get('job', {})
        products = job_data.get('products', {}) or {}
        bullets = products.get('bullets', [])
        questionnaire = products.get('questionnaire', {}) or {}
        questions = questionnaire.get('questions', [])

        return {
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


class ThreadedCrawler:
    def __init__(self, job_ids: List[str], thread_id: int, folder_name: str, crawler_instance):
        self.job_ids = job_ids
        self.thread_id = thread_id
        self.folder_name = folder_name
        self.crawler_instance = crawler_instance
        self.thread_request_count = 0
        self._init_user_config()  # 每次实例化生成新配置

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
        self._init_user_config()  # 达到频率时刷新配置
        print(f"线程 {self.thread_id} 更换配置")  # 符合第5项通知要求
        self.thread_request_count = 0

    def process_jobs(self):
        success_count = 0
        failed_ids = []
        start_time = time.time()

        # 只需创建一次临时文件夹
        thread_temp_folder = f'./{self.folder_name}/thread_{self.thread_id}_temp'
        os.makedirs(thread_temp_folder, exist_ok=True)

        for idx, job_id in enumerate(self.job_ids):
            file_path = f'{thread_temp_folder}/{job_id}.json'

            scheduled_time = start_time + (idx * 3 + self.thread_id * 0.5)
            if (delay := scheduled_time - time.time()) > 0:
                time.sleep(delay)

            try:
                data = get_job_details(job_id, self)
                if data and data.get('data', {}).get('jobDetails', {}).get('job'):
                    # 创建线程专属临时文件夹
                    thread_temp_folder = f'./{self.folder_name}/thread_{self.thread_id}_temp'
                    os.makedirs(thread_temp_folder, exist_ok=True)

                    # 新增有效性检查
                    file_path = f'{thread_temp_folder}/{job_id}.json'
                    if os.path.exists(file_path):  # 清理可能存在的旧文件
                        os.remove(file_path)

                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                        f.flush()  # 确保写入完成

                    # 写入后验证文件是否真正存在
                    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                        success_count += 1
                    else:
                        failed_ids.append(job_id)
                else:
                    # 删除可能存在的无效文件
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    failed_ids.append(job_id)
            except Exception as e:
                # 删除可能存在的部分写入文件
                if 'file_path' in locals() and os.path.exists(file_path):
                    os.remove(file_path)
                failed_ids.append(job_id)

            # 用户配置更新逻辑保持不变
            self.thread_request_count += 1
            if self.thread_request_count >= self.crawler_instance.config.user_switch_freq:
                self._rotate_user_config()

            with self.crawler_instance.lock:
                self.crawler_instance.total_processed += 1
                if self.crawler_instance.total_processed % self.crawler_instance.config.job_progress_step == 0:
                    print(
                        f"总进度: {self.crawler_instance.total_processed}/{len(self.crawler_instance.all_ids)}")  # 更改引用

        return success_count, failed_ids


if __name__ == "__main__":
    # 使用默认配置
    config = CrawlerConfig()  # ✔️ 参数默认值生效
    # 自定义配置示例（已注释）
    # config = CrawlerConfig(num_threads=12)  # ✔️ 可自定义
    crawler = JobsDBCrawler(config)  # ✔️ 配置传入
    crawler.crawl_all_pages(total_pages)
