import logging
from pathlib import Path  # 添加 Path 导入
import os
from datetime import datetime


def setup_logger(log_level=logging.INFO, log_to_file=True, log_dir='logs'):
    """
    设置日志系统

    参数:
        log_level: 日志级别 (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR)
        log_to_file: 是否输出到文件
        log_dir: 日志文件目录

    返回:
        配置好的logger对象
    """
    # 创建logger
    logger = logging.getLogger('jobcrawler')

    # 移除现有的处理器，防止重复输出
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(log_level)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    # 设置日志格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)

    # 添加控制台处理器
    logger.addHandler(console_handler)

    # 如果需要文件输出
    if log_to_file:
        # 确保日志目录存在
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(exist_ok=True, parents=True)

        # 日志文件名：包含日期时间
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir_path / f'crawler_{timestamp}.log'

        # 创建文件处理器
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)

        # 添加文件处理器
        logger.addHandler(file_handler)

    return logger


# 创建默认logger实例
logger = setup_logger()
