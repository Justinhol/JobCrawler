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

total_pages = 0
pageSize = 100  # 最大只能100


class JobsDBCrawler:
    # 用户信息池
    cookies = [
        '_fbp=fb.1.1739346865000.173205080756887697; _dd_s=rum=0&expire=1739416205000&logs=0; sol_id=1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d',
        '_fbp=fb.1.1739346872000.298333333333333333; _dd_s=rum=0&expire=1739416212000&logs=0; sol_id=7d8e9f0a-1b2c-3d4e-5f6a-7b8c9d0e1f2a',
        '_fbp=fb.1.1739346878500.414213562373095048; _dd_s=rum=0&expire=1739416218500&logs=0; sol_id=3c4d5e6f-7a8b-9c0d-1e2f-3a4b5c6d7e8f',
        '_fbp=fb.1.1739346884000.732050807568877293; _dd_s=rum=0&expire=1739416224000&logs=0; sol_id=9a8b7c6d-5e4f-3a2b-1c0d-9e8f7a6b5c4d',
        '_fbp=fb.1.1739346891234.236067977499789696; _dd_s=rum=0&expire=1739416231234&logs=0; sol_id=2e3f4a5b-6c7d-8e9f-0a1b-2c3d4e5f6a7b',
        '_fbp=fb.1.1739346898000.577215664901532860; _dd_s=rum=0&expire=1739416238000&logs=0; sol_id=5d6e7f8a-9b0c-1d2e-3f4a-5b6c7d8e9f0a',
        '_fbp=fb.1.1739346904567.141592653589793238; _dd_s=rum=0&expire=1739416244567&logs=0; sol_id=0f1e2d3c-4b5a-6978-8c7d-6e5f4a3b2c1d',
        '_fbp=fb.1.1739346910000.718281828459045235; _dd_s=rum=0&expire=1739416250000&logs=0; sol_id=1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d',
        '_fbp=fb.1.1739346916789.161803398874989484; _dd_s=rum=0&expire=1739416256789&logs=0; sol_id=9e8f7a6b-5c4d-3e2f-1a0b-9c8d7e6f5a4b',
        '_fbp=fb.1.1739346923000.314159265358979323; _dd_s=rum=0&expire=1739416263000&logs=0; sol_id=3b4c5d6e-7f8a-9b0c-1d2e-3f4a5b6c7d8e',
        '_fbp=fb.1.1739346929500.414213562373095048; _dd_s=rum=0&expire=1739416269500&logs=0; sol_id=6d5e4f3a-2b1c-0d9e-8f7a-6b5c4d3e2f1a',
        '_fbp=fb.1.1739346936000.618033988749894848; _dd_s=rum=0&expire=1739416276000&logs=0; sol_id=4e5f6a7b-8c9d-0e1f-2a3b-4c5d6e7f8a9b',
        '_fbp=fb.1.1739346942000.707106781186547524; _dd_s=rum=0&expire=1739416282000&logs=0; sol_id=7f8a9b0c-1d2e-3f4a-5b6c-7d8e9f0a1b2c',
        '_fbp=fb.1.1739346948500.173205080756887697; _dd_s=rum=0&expire=1739416288500&logs=0; sol_id=3d4e5f6a-7b8c-9d0e-1f2a-3b4c5d6e7f8a',
        '_fbp=fb.1.1739346955000.236067977499789696; _dd_s=rum=0&expire=1739416295000&logs=0; sol_id=8b9c0d1e-2f3a-4b5c-6d7e-8f9a0b1c2d3e',
        '_fbp=fb.1.1739346961234.298333333333333333; _dd_s=rum=0&expire=1739416301234&logs=0; sol_id=5a6b7c8d-9e0f-1a2b-3c4d-5e6f7a8b9c0d',
        '_fbp=fb.1.1739346967890.332333333333333333; _dd_s=rum=0&expire=1739416307890&logs=0; sol_id=0e1f2a3b-4c5d-6e7f-8a9b-0c1d2e3f4a5b',
        '_fbp=fb.1.1739346974000.414213562373095048; _dd_s=rum=0&expire=1739416314000&logs=0; sol_id=6c7d8e9f-0a1b-2c3d-4e5f-6a7b8c9d0e1f',
        '_fbp=fb.1.1739346980500.577215664901532860; _dd_s=rum=0&expire=1739416320500&logs=0; sol_id=9d0e1f2a-3b4c-5d6e-7f8a-9b0c1d2e3f4a',
        '_fbp=fb.1.1739346987000.718281828459045235; _dd_s=rum=0&expire=1739416327000&logs=0; sol_id=2b3c4d5e-6f7a-8b9c-0d1e-2f3a4b5c6d7e',
        '_fbp=fb.1.1739346810799.557832144221994211; _dd_s=rum=0&expire=1739416135200&logs=0; sol_id=8a4e6b2c-5d1e-4a89-bb32-f7c0a9d3e1f2',
        '_fbp=fb.1.1739346811234.884215639872155632; _dd_s=rum=0&expire=1739416136123&logs=0; sol_id=3b9f7ac8-21cd-4e76-9e8d-1a45b6c7d0a4',
        '_fbp=fb.1.1739346815555.112358132134558914; _dd_s=rum=0&expire=1739416140000&logs=0; sol_id=6e2d1c47-0a8b-4d2f-9c3a-5f8e9b6d0e1a',
        '_fbp=fb.1.1739346820000.271828182845904523; _dd_s=rum=0&expire=1739416145000&logs=0; sol_id=9c7a8d2b-4e3f-4a5c-b6d1-2f3e4a5b6c7d',
        '_fbp=fb.1.1739346829876.314159265358979323; _dd_s=rum=0&expire=1739416152000&logs=0; sol_id=1d2e3f4a-5b6c-7d8e-9f0a-1b2c3d4e5f6a',
        '_fbp=fb.1.1739346836543.161803398874989484; _dd_s=rum=0&expire=1739416161000&logs=0; sol_id=a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d',
        '_fbp=fb.1.1739346842012.577215664901532860; _dd_s=rum=0&expire=1739416170500&logs=0; sol_id=b3c4d5e6-f7a8-49b0-8c1d-2e3f4a5b6c7d',
        '_fbp=fb.1.1739346850123.618033988749894848; _dd_s=rum=0&expire=1739416180000&logs=0; sol_id=4d5e6f7a-8b9c-4d0e-9f1a-2b3c4d5e6f7a',
        '_fbp=fb.1.1739346854321.141421356237309504; _dd_s=rum=0&expire=1739416199999&logs=0; sol_id=7e8f9a0b-1c2d-3e4f-5a6b-7c8d9e0f1a2b',
        '_fbp=fb.1.1739346860000.707106781186547524; _dd_s=rum=0&expire=1739416200000&logs=0; sol_id=0d1e2f3a-4b5c-6d7e-8f9a-0b1c2d3e4f5a'
    ]

    session_ids = [
        "a0b1c2d3-e4f5-6a7b-8c9d-0e1f2a3b4c5d",
        "f1e2d3c4-b5a6-7c8d-9e0f-1a2b3c4d5e6f",
        "9e8f7a6b-5c4d-3e2f-1a0b-9c8d7e6f5a4b",
        "3d4e5f6a-7b8c-9d0e-1f2a-3b4c5d6e7f8a",
        "5c6d7e8f-9a0b-1c2d-3e4f-5a6b7c8d9e0f",
        "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d",
        "7d8e9f0a-1b2c-3d4e-5f6a-7b8c9d0e1f2a",
        "4b5c6d7e-8f9a-0b1c-2d3e-4f5a6b7c8d9e",
        "0f1e2d3c-4b5a-6978-8c7d-6e5f4a3b2c1d",
        "8b9c0d1e-2f3a-4b5c-6d7e-8f9a0b1c2d3e",
        "6a7b8c9d-0e1f-2a3b-4c5d-6e7f8a9b0c1d",
        "3e4f5a6b-7c8d-9e0f-1a2b-3c4d5e6f7a8b",
        "9b0c1d2e-3f4a-5b6c-7d8e-9f0a1b2c3d4e",
        "2d3e4f5a-6b7c-8d9e-0f1a-2b3c4d5e6f7a",
        "7e8f9a0b-1c2d-3e4f-5a6b-7c8d9e0f1a2b",
        "4c5d6e7f-8a9b-0c1d-2e3f-4a5b6c7d8e9f",
        "1e2f3a4b-5c6d-7e8f-9a0b-1c2d3e4f5a6b",
        "5d6e7f8a-9b0c-1d2e-3f4a-5b6c7d8e9f0a",
        "0b1c2d3e-4f5a-6b7c-8d9e-0f1a2b3c4d5e",
        "47cde2e6-288a-41bb-acb9-90c38d3cca55",
        "5a9f1b2c-3d4e-4f5a-6b7c-8d9e0f1a2b3c",
        "e6f7a8b9-c0d1-2e3f-4a5b-6c7d8e9f0a1b",
        "2c3d4e5f-6a7b-8c9d-0e1f-2a3b4c5d6e7f",
        "8a9b0c1d-2e3f-4a5b-6c7d-8e9f0a1b2c3d",
        "4e5f6a7b-8c9d-0e1f-2a3b-4c5d6e7f8a9b",
        "0a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d",
        "6e7f8a9b-0c1d-2e3f-4a5b-6c7d8e9f0a1b",
        "3b4c5d6e-7f8a-9b0c-1d2e-3f4a5b6c7d8e",
        "9f0a1b2c-3d4e-5f6a-7b8c-9d0e1f2a3b4c"
    ]

    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_8) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_16_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/20.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/20.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/20.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/21.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/21.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/21.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/22.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/22.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/22.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/23.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/23.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 15_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/23.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 16_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/24.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 16_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/24.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 16_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/24.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 17_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/25.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 17_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/25.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 17_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/25.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 18_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 18_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.1 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 18_2_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 19_0_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.0 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 19_1_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/27.1 Safari/605.1.15"
    ]

    def __init__(self):
        self.base_url = "https://hk.jobsdb.com/api/jobsearch/v5/search"
        self.request_count = 0
        self.current_user_index = 0
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh-Hans;q=0.9",
            "User-Agent": self.user_agents[self.current_user_index],
            "Referer": "https://hk.jobsdb.com/jobs-in-science-technology",
            "seek-request-brand": "jobsdb",
            "seek-request-country": "HK",
        }
        self.params = {
            "siteKey": "HK-Main",
            "sourcesystem": "houston",
            "classification": "1223",
            "sortmode": "ListedDate",
            "pageSize": pageSize,
            "include": "seodata,relatedsearches,joracrosslink,gptTargeting",
            "locale": "en-HK",
        }

    def get_user_info(self):
        """获取当前用户信息"""
        # 每1000次请求更换一次用户信息
        if self.request_count >= 50:
            self.current_user_index = (
                self.current_user_index + 1) % len(self.cookies)
            self.request_count = 0
            print(f"信息: 切换到用户配置 {self.current_user_index + 1}")
            # 更新headers中的User-Agent
            self.headers["User-Agent"] = self.user_agents[self.current_user_index]

        self.request_count += 1
        return {
            'cookie': self.cookies[self.current_user_index],
            'session_id': self.session_ids[self.current_user_index],
            'user_agent': self.user_agents[self.current_user_index]
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
            data = self.fetch_page(page)
            if data:
                for jobs in data["data"]:
                    all_ids.append(jobs["solMetadata"]["jobId"])
            else:
                print(f"警告: 第 {page} 页数据获取失败")

            # 每爬取10页打印一次进度
            if page % 10 == 0 or page == total_pages:
                print(f"页面爬取进度: {page}/{total_pages}")
            time.sleep(random.uniform(1.5, 2.5))

        # 反转ID列表，使最早发布的职位在前
        all_ids.reverse()

        # 创建交错分配的分块（线程0拿第0,6,12...个元素，线程1拿第1,7,13...个元素）
        job_chunks = [[] for _ in range(6)]
        for idx, job_id in enumerate(all_ids):
            thread_idx = idx % 6
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

        for i in range(6):
            thread = ThreadedCrawler(job_chunks[i], i, folder_name, self)
            threads.append(thread)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
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

        # 更新文件路径为新的文件夹名称
        for i in range(6):
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
        self.request_count = 0
        self.current_user_index = thread_id  # 每个线程使用不同的用户配置

    def process_jobs(self):
        csv_file = f'./{self.folder_name}/thread_{self.thread_id}.csv'
        success_count = 0
        failed_ids = []
        start_time = time.time()  # 记录全局时间基准

        for idx, job_id in enumerate(self.job_ids):
            # 计算每个请求的预定发送时间
            scheduled_time = start_time + (idx * 3) + (self.thread_id * 0.5)
            now = time.time()
            delay = scheduled_time - now
            if delay > 0:
                time.sleep(delay)

            try:
                if self.request_count >= 50:
                    self.current_user_index = (
                        self.current_user_index + 10) % len(self.crawler_instance.cookies)
                    self.request_count = 0
                    print(
                        f"信息: 线程 {self.thread_id} 切换到用户配置 {self.current_user_index + 1}")

                data = get_job_details(
                    job_id, self.crawler_instance, user_index=self.current_user_index)
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
                    self.request_count += 1

                    # 每500个职位打印一次进度
                    if success_count % 10 == 0:
                        print(
                            f"线程 {self.thread_id} 进度: {success_count}/{len(self.job_ids)}")
                else:
                    failed_ids.append(job_id)

            except Exception as e:
                print(f"错误: 线程 {self.thread_id} 处理 ID {job_id} 时失败: {str(e)}")
                failed_ids.append(job_id)

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


if __name__ == "__main__":
    crawler = JobsDBCrawler()
    crawler.crawl_all_pages(total_pages)
