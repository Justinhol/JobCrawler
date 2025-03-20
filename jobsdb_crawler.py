import requests
import json
import time
import math
from typing import Dict, Any, List
from jobsdb_job_details import get_job_details, safe_get
import random
from pathlib import Path  # 导入 Path 以替代 os.path
import os
from datetime import datetime
import csv
import pandas as pd
import threading
from queue import Queue
import concurrent.futures
import uuid
import logging
from logger_config import logger, setup_logger

total_pages = 0
pageSize = 100  # 最大只能100


class CrawlerConfig:
    """双进度配置"""

    def __init__(
        self,
        num_threads=6,            # 并发线程数
        user_switch_freq=3000,       # 用户切换频率
        page_progress_step=10,     # 页面进度打印间隔
        job_progress_step=500,      # 职位进度打印间隔
        log_level="INFO",          # 日志级别
        log_to_file=True           # 是否输出到文件
    ):
        self.num_threads = num_threads
        self.user_switch_freq = user_switch_freq
        self.page_progress_step = page_progress_step
        self.job_progress_step = job_progress_step
        self.log_level = log_level
        self.log_to_file = log_to_file


class JobsDBCrawler:
    def __init__(self, config=CrawlerConfig()):
        self.config = config
        # 配置日志
        log_level = getattr(logging, config.log_level.upper(), logging.INFO)
        global logger
        logger = setup_logger(log_level=log_level,
                              log_to_file=config.log_to_file)

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
            # "classification": "2C1206,6058",  # 30
            # "classification": "1223", # 400+
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
            logger.info(f"生成新用户配置 {self.current_user_index + 1}")
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
                    logger.warning(f"第{page}页重试中（第{attempt}次）...")

                response = requests.get(
                    self.base_url,
                    headers=self.headers,
                    params=self.params
                )
                response.raise_for_status()

                # 添加成功打印
                if attempt > 0:
                    logger.info(f"第{page}页第{attempt}次重试成功")
                return response.json()

            except requests.RequestException as e:
                logger.error(f"获取第 {page} 页时失败: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(3)  # 失败后等待3秒

        # 所有重试失败后打印
        logger.error(f"第{page}页所有{max_retries}次重试均失败")
        return {}

    def crawl_all_pages(self, total_pages):
        """爬取所有页面的数据"""
        # 移除 all_ids 局部变量，使用类的实例变量 self.all_ids
        # 爬取第一页获取职位总数
        data = self.fetch_page(1)
        self.total_jobs = data["solMetadata"]["totalJobCount"]
        # 计算总页数
        total_pages = math.ceil(self.total_jobs / pageSize)
        logger.info(f"开始爬取，共 {self.total_jobs} 个职位，{total_pages} 页")
        time.sleep(1)

        # 保持data目录下的路径结构
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        folder_name = Path("data") / f"{current_time}_temp"  # 使用 Path 处理路径
        folder_name.mkdir(parents=True, exist_ok=True)  # 创建目录

        # 直接创建original_data目录
        original_data_folder = folder_name / 'original_data'
        original_data_folder.mkdir(exist_ok=True)

        # 第一阶段：获取所有ID
        for page in range(1, total_pages + 1):
            if page % self.config.page_progress_step == 0:
                logger.info(f"页面进度: {page}/{total_pages}")
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
        logger.info(
            f"开始抓取 {len(self.all_ids)} 个职位，已去重 {len_before - len(self.all_ids)} 个")

        # 处理阶段
        total_success, all_failed_ids = self.process_jobs_with_configpool(
            self.all_ids,
            original_data_folder,
            max_retries=1
        )

        if all_failed_ids:
            logger.info(f"开始重试失败的 {len(all_failed_ids)} 个职位")
            retry_success, final_failed_ids = self.process_jobs_with_configpool(
                all_failed_ids,
                original_data_folder,
                max_retries=1,
                is_retry=True
            )
            total_success += retry_success
            all_failed_ids = final_failed_ids
            logger.info(
                f"重试结果: 成功 {retry_success} 个，仍失败 {len(final_failed_ids)} 个")

        # 记录失败的ID
        if all_failed_ids:
            # 使用原子写入方式保存失败的ID
            failed_ids_path = folder_name / 'failed_ids.json'
            temp_failed_path = folder_name / 'failed_ids.tmp.json'
            try:
                with open(temp_failed_path, 'w', encoding='utf-8') as f:
                    json.dump(all_failed_ids, f, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                temp_failed_path.rename(failed_ids_path)
            except Exception as e:
                logger.error(f"保存失败ID时出错: {str(e)}")
                if temp_failed_path.exists():
                    temp_failed_path.unlink()

        # 重命名文件夹（使用实际成功数量）
        new_folder_name = Path("data") / f"{current_time}_{total_success}"
        folder_name.rename(new_folder_name)

        # 最终统计
        # 添加实际文件计数来验证统计准确性
        original_data_folder = new_folder_name / 'original_data'
        actual_json_count = sum(
            1 for f in original_data_folder.iterdir() if f.suffix == '.json')

        logger.info(f"JSON数据爬取完成!")
        logger.info(f"总数: {len(self.all_ids)}")
        logger.info(f"JSON存储成功: {total_success}")
        logger.info(f"实际JSON文件数: {actual_json_count}")  # 添加实际文件数统计
        logger.info(f"最终失败: {len(all_failed_ids)}")

        # 存储最新文件夹路径供后续使用
        self.latest_folder = new_folder_name

    def process_csv_extraction(self, folder_name=None):
        """统一处理CSV提取、重试和合并的流程"""
        # 如果没有提供文件夹名，使用最新的文件夹
        if not folder_name:
            folder_name = self.latest_folder
            if not folder_name:
                folder_name = self.get_latest_data_folder()
                if not folder_name:
                    logger.error("未找到数据文件夹")
                    return

        logger.info(f"开始处理文件夹: {folder_name}")

        # 1. 提取CSV
        logger.info("开始提取所有JSON到CSV...")
        csv_success, csv_failed = self.extract_all_json_to_csv(folder_name)
        total_csv_success = csv_success

        # 2. 重试失败的提取
        if csv_failed:
            logger.info(f"开始重试失败的 {len(csv_failed)} 个CSV提取")
            retry_success, still_failed = self.extract_all_json_to_csv(
                folder_name, csv_failed)
            total_csv_success += retry_success
            logger.info(
                f"CSV重试结果: 成功 {retry_success} 个，仍失败 {len(still_failed)} 个")
            if still_failed:
                # 使用原子写入保存CSV提取失败的ID
                csv_failed_path = Path(folder_name) / 'csv_failed.json'
                temp_csv_failed_path = Path(
                    folder_name) / 'csv_failed.tmp.json'
                try:
                    with open(temp_csv_failed_path, 'w', encoding='utf-8') as f:
                        json.dump(still_failed, f, ensure_ascii=False)
                        f.flush()
                        os.fsync(f.fileno())
                    temp_csv_failed_path.rename(csv_failed_path)
                except Exception as e:
                    logger.error(f"保存CSV提取失败ID时出错: {str(e)}")
                    if temp_csv_failed_path.exists():
                        temp_csv_failed_path.unlink()

        # 3. 合并CSV文件件
        logger.info("重命名CSV文件...")
        self.rename_csv_file(folder_name)

        # 4. 打印统计信息
        original_data_folder = Path(folder_name) / 'original_data'
        actual_json_count = sum(
            1 for f in original_data_folder.iterdir() if f.suffix == '.json')

        logger.info("CSV提取完成!")
        logger.info(f"CSV提取成功: {total_csv_success}")
        logger.info(f"JSON文件与CSV提取差异: {actual_json_count - total_csv_success}")
        logger.info(
            f"CSV提取失败: {len(still_failed) if 'still_failed' in locals() else 0}")

    def get_latest_data_folder(self):
        """获取data目录下最新的数据文件夹"""
        data_dir = Path("data")
        if not data_dir.exists():
            return None

        folders = [f for f in data_dir.iterdir() if f.is_dir()
                   and f.name[0].isdigit()]
        if not folders:
            return None

        # 按照时间戳排序，返回最新的
        folders.sort(reverse=True)
        return str(folders[0])

    def create_user_config_pool(self):
        """创建用户配置池，模拟多线程效果"""
        config_pool = []
        for i in range(self.config.num_threads):
            config = {
                'user_agent': self._generate_user_agent(),
                'cookie': self._generate_cookie(),
                'session_id': self._generate_session_id(),
                'request_count': 0
            }
            config_pool.append(config)
        return config_pool

    def process_jobs_with_configpool(self, job_ids, original_data_folder, max_retries=1, is_retry=False):
        """使用配置池处理职位（模拟多线程）"""
        total_success = 0
        all_failed_ids = []

        # 创建用户配置池
        user_config_pool = self.create_user_config_pool()
        pool_size = len(user_config_pool)

        # 计数器和进度跟踪
        total_processed = 0
        start_time = time.time()

        for attempt in range(max_retries):
            logger.info(f"\n{'重试' if is_retry else '处理'}阶段（第{attempt+1}次尝试）")

            # 设定配置池刷新计数器
            pool_refresh_counter = 0

            for idx, job_id in enumerate(job_ids):
                # 选择当前使用的配置
                config_idx = idx % pool_size
                current_config = user_config_pool[config_idx]

                # 计算请求时间间隔，模拟多线程效果
                scheduled_time = start_time + (idx * 0.5)
                if (delay := scheduled_time - time.time()) > 0:
                    time.sleep(delay)

                # 每个配置计数
                current_config['request_count'] += 1
                pool_refresh_counter += 1

                # 每到指定次数刷新整个配置池
                if pool_refresh_counter >= self.config.user_switch_freq:
                    logger.info("刷新用户配置池")
                    user_config_pool = self.create_user_config_pool()
                    pool_refresh_counter = 0

                # 处理单个职位
                original_data_path = Path(original_data_folder)
                final_path = original_data_path / f'{job_id}.json'
                temp_path = original_data_path / f'{job_id}.tmp'  # 临时文件路径

                try:
                    # 创建伪线程爬虫对象，传递当前配置
                    pseudo_crawler = PseudoThreadedCrawler(current_config)

                    data = get_job_details(job_id, pseudo_crawler)
                    if data and data.get('data', {}).get('jobDetails', {}).get('job'):
                        # 删除可能存在的旧文件
                        for path in [temp_path, final_path]:
                            if path.exists():
                                path.unlink()

                        # 先写入临时文件
                        with open(temp_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                            f.flush()  # 确保写入完成
                            os.fsync(f.fileno())  # 确保数据刷新到磁盘

                        # 完成后重命名为最终文件
                        temp_path.rename(final_path)

                        # 验证文件是否正确写入
                        if final_path.exists() and final_path.stat().st_size > 0:
                            total_success += 1
                        else:
                            all_failed_ids.append(job_id)
                    else:
                        # 删除可能存在的无效文件
                        for path in [temp_path, final_path]:
                            if path.exists():
                                path.unlink()
                        all_failed_ids.append(job_id)
                except Exception as e:
                    # 清理临时文件和可能的部分写入文件
                    for path in [temp_path, final_path]:
                        if path.exists():
                            path.unlink()
                    all_failed_ids.append(job_id)
                    logger.error(
                        f"{len(all_failed_ids)}: 处理职位 {job_id} 失败: {str(e)}")

                # 更新进度
                total_processed += 1
                if total_processed % self.config.job_progress_step == 0:
                    logger.info(f"总进度: {total_processed}/{len(job_ids)}")

            # 第一轮处理完成后，如果没有失败的职位，则不需要第二轮
            if not all_failed_ids:
                break

        return total_success, all_failed_ids

    def rename_csv_file(self, folder_name):
        # 使用原子方式重命名
        folder_path = Path(folder_name)
        source = folder_path / 'all_jobs.csv'
        target = folder_path / f"{folder_path.name}.csv"
        temp_target = folder_path / f"{folder_path.name}.tmp.csv"

        if source.exists():
            try:
                # 如果目标已存在，先复制到临时文件
                with open(source, 'rb') as src_file:
                    with open(temp_target, 'wb') as tmp_file:
                        tmp_file.write(src_file.read())
                        tmp_file.flush()
                        os.fsync(tmp_file.fileno())

                # 验证临时文件完整性
                if temp_target.exists() and temp_target.stat().st_size > 0:
                    if target.exists():
                        target.unlink()
                    temp_target.rename(target)
                    source.unlink()  # 删除原始文件
                    logger.info(f"已重命名CSV文件: {target}")
            except Exception as e:
                logger.error(f"重命名CSV文件失败: {str(e)}")
                if temp_target.exists():
                    temp_target.unlink()

    def extract_all_json_to_csv(self, folder_name, failed_files=None):
        """从JSON文件统一提取CSV"""
        folder_path = Path(folder_name)
        temp_csv_path = folder_path / 'all_jobs.tmp.csv'
        final_csv_path = folder_path / 'all_jobs.csv'
        json_folder = folder_path / 'original_data'  # 路径已自动包含data目录
        success_count = 0
        local_failed = []

        # 获取实际JSON文件数量
        actual_json_count = sum(
            1 for f in json_folder.iterdir() if f.suffix == '.json')

        try:
            with open(temp_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = None
                # 根据是否重试决定处理文件列表
                if failed_files:
                    files_to_process = [f"{jid}.json" for jid in failed_files]
                else:
                    files_to_process = [
                        f.name for f in json_folder.iterdir() if f.is_file()]

                # 打印实际处理文件数量信息
                logger.info(
                    f"实际JSON文件数量: {actual_json_count}, 即将处理文件数量: {len([f for f in files_to_process if f.endswith('.json')])}")

                for filename in files_to_process:
                    if not filename.endswith('.json'):
                        continue

                    try:
                        with open(json_folder / filename, 'r', encoding='utf-8') as f:
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
                        logger.error(f"提取 {filename} 失败: {str(e)}")
                        local_failed.append(filename.split('.')[
                                            0])  # 保存不带扩展名的job_id

            # 写入成功后重命名为最终文件
            if temp_csv_path.exists():
                # 如果已有最终文件，先删除
                if final_csv_path.exists():
                    final_csv_path.unlink()
                temp_csv_path.rename(final_csv_path)
        except Exception as e:
            logger.error(f"创建CSV文件失败: {str(e)}")
            if temp_csv_path.exists():
                temp_csv_path.unlink()
            raise

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


class PseudoThreadedCrawler:
    """伪线程爬虫类，模拟ThreadedCrawler的接口"""

    def __init__(self, user_config):
        self.user_config = user_config

    def get_thread_user_info(self):
        """返回用户配置信息"""
        return {
            'cookie': self.user_config['cookie'],
            'session_id': self.user_config['session_id'],
            'user_agent': self.user_config['user_agent']
        }


if __name__ == "__main__":
    # 使用默认配置
    config = CrawlerConfig()  # ✔️ 参数默认值生效
    # 自定义配置示例（已注释）
    # config = CrawlerConfig(num_threads=12)  # ✔️ 可自定义
    crawler = JobsDBCrawler(config)  # ✔️ 配置传入

    # 爬取数据
    crawler.crawl_all_pages(total_pages)

    # 一步完成所有CSV处理
    crawler.process_csv_extraction()
