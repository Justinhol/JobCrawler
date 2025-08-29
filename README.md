# JobCrawler - JobsDB 香港职位爬虫

一个高效、稳定的 JobsDB 香港职位信息爬虫工具，支持大规模数据采集和自动重试机制。

## 🚀 功能特性

- **高效并发**: 支持多线程配置池，模拟真实用户行为
- **智能重试**: 两轮处理机制，第一轮快速处理，第二轮重试失败任务
- **超时控制**: 第一轮 6 秒超时，重试阶段 15 秒超时，避免网络阻塞
- **用户伪装**: 动态生成 User-Agent、Cookie、Session ID，每 500 个任务自动切换
- **数据完整性**: 原子文件操作，确保数据一致性
- **详细日志**: 实时进度监控，失败计数，性能统计
- **自动化流程**: 从职位列表获取到 CSV 提取的全自动化处理

## 📋 系统要求

- Python 3.8+
- macOS / Linux / Windows
- 网络连接

## 🛠 安装依赖

```bash
pip install -r requirements.txt
```

### 主要依赖包

```
requests>=2.25.1
pandas>=1.3.0
pathlib
uuid
logging
concurrent.futures
```

## ⚙️ 配置说明

### CrawlerConfig 参数

```python
config = CrawlerConfig(
    num_threads=6,          # 并发线程数 (建议6-12)
    user_switch_freq=500,   # 每500个任务切换用户配置
    page_progress_step=10,  # 每10页打印页面进度
    job_progress_step=100,  # 每100个职位打印处理进度
    log_level="INFO",       # 日志级别 (DEBUG/INFO/WARNING/ERROR)
    log_to_file=True        # 是否保存日志到文件
)
```

### 分类配置

当前抓取分类（可在代码中修改）：

```python
# 所有IT相关分类
"classification": "6123,6281,1200,6251,6304,1203,1204,1225,6246,6261,1223,6362,6043,1220,6058,6008,6092,1216,1214,6317,1212,1211,1210,6205,1209,6263,6076,1206,6163,7019"

# 示例：仅抓取特定分类
# "classification": "1223"  # 约400+职位
```

## 🚀 快速开始

### 基本使用

```python
from jobsdb_crawler import JobsDBCrawler, CrawlerConfig

# 使用默认配置
crawler = JobsDBCrawler()
crawler.crawl_all_pages(total_pages=0)  # 0表示自动获取总页数
crawler.process_csv_extraction()
```

### 自定义配置

```python
# 高性能配置
config = CrawlerConfig(
    num_threads=12,         # 更多线程
    job_progress_step=50,   # 更频繁的进度报告
    log_level="DEBUG"       # 详细日志
)

crawler = JobsDBCrawler(config)
crawler.crawl_all_pages(total_pages=0)
crawler.process_csv_extraction()
```

## 📊 处理流程

### 1. 初始化阶段

- 动态生成用户配置 (User-Agent, Cookie, Session ID)
- 获取第一页，确定总职位数和总页数
- 创建临时文件夹：`data/YYYYMMDDHHMMSS_temp`

### 2. 职位 ID 收集

- 遍历所有页面，间隔 1 秒
- 提取所有职位 ID，去重并反转（最新职位优先）
- 按配置间隔打印页面进度

### 3. 第一轮处理（快速模式）

- **6 秒超时**：快速跳过网络阻塞请求
- **0.1 秒间隔**：高速处理职位详情
- **失败直接记录**：不重试，记录失败 ID
- **每 500 个任务**：刷新用户配置池

### 4. 重试阶段

- **15 秒超时**：给予失败任务更多时间
- 只处理第一轮失败的职位
- 最终失败的职位保存到 `failed_ids.json`

### 5. CSV 提取

- 统一处理所有 JSON 文件
- 提取关键字段到 CSV
- 失败提取的文件进行重试
- 生成最终的数据文件

## 📁 输出结构

```
data/
└── YYYYMMDDHHMMSS_XXXXX/        # 最终文件夹 (XXXXX为成功数量)
    ├── original_data/           # 原始JSON数据
    │   ├── 12345678.json       # 职位详情JSON
    │   └── ...
    ├── YYYYMMDDHHMMSS_XXXXX.csv # 最终CSV文件
    ├── failed_ids.json         # 失败的职位ID (如果有)
    └── csv_failed.json         # CSV提取失败的ID (如果有)
```

## 📈 性能优化

### 速度优化历程

- **原版本**: 40k 数据需要 18 小时
- **优化后**: 40k 数据仅需 3-5 小时

### 优化策略

1. **请求间隔**: 从 0.5 秒降至 0.1 秒 (5 倍提速)
2. **超时控制**: 6 秒快速超时，避免无限等待
3. **智能重试**: 分两轮处理，避免重复重试
4. **配置池**: 每 500 个任务刷新，避免被检测

## 📋 数据字段

生成的 CSV 包含以下字段：

| 字段名          | 描述     | 示例                          |
| --------------- | -------- | ----------------------------- |
| job_id          | 职位 ID  | 12345678                      |
| classifications | 职位分类 | Information Technology        |
| title           | 职位标题 | Senior Software Engineer      |
| post_time       | 发布时间 | 2024-01-15T10:30:00Z          |
| expires_time    | 过期时间 | 2024-02-15T10:30:00Z          |
| abstract        | 职位摘要 | Join our dynamic team...      |
| content         | 职位详情 | We are looking for...         |
| salary          | 薪资范围 | HK$50,000 - HK$70,000         |
| link            | 职位链接 | https://hk.jobsdb.com/job/... |
| work_types      | 工作类型 | Full Time                     |
| advertiser      | 招聘公司 | ABC Technology Ltd            |
| location        | 工作地点 | Central, Hong Kong            |
| bullets         | 要点列表 | Python,AWS,Docker             |
| questions       | 筛选问题 | Years of experience?          |

## 🔍 日志监控

### 进度日志示例

```
2024-01-15 10:30:00,123 - INFO - 开始爬取，共 45000 个职位，450 页
2024-01-15 10:31:00,456 - INFO - 第一轮处理：6秒超时，快速跳过阻塞请求
2024-01-15 10:32:00,789 - INFO - 总进度: 1000/45000 (2.2%) - 成功率: 94.5% - 失败: 55个(5.5%) - 速度: 16.7/秒 - 预计剩余: 2640秒 - 6秒超时策略
2024-01-15 10:33:00,012 - INFO - 已处理 500 个任务，刷新用户配置池
```

### 失败日志示例

```
2024-01-15 10:34:00,345 - WARNING - 第20个失败: 职位 85720181 请求超时 6.2秒 (超时设置: 6秒)
2024-01-15 10:35:00,678 - INFO - 失败汇总: 已失败 100 个，当前失败率 4.2%
```

## 🛡️ 反检测机制

1. **动态用户信息**：

   - 随机生成 Safari User-Agent
   - 动态创建 Cookie 和 Session ID
   - 每 500 个请求自动切换

2. **请求频率控制**：

   - 0.1 秒基础间隔
   - 模拟多线程请求时序
   - 失败自动降速

3. **异常处理**：
   - 429 频率限制自动等待
   - 网络异常自动重试
   - 连接池管理

## 🐛 故障排除

### 常见问题

**问题**: 大量超时失败

```bash
# 解决方案：降低并发数
config = CrawlerConfig(num_threads=3)
```

**问题**: 被频率限制

```bash
# 解决方案：增加间隔时间
# 修改代码中的 0.1 秒间隔为 0.2 秒
```

**问题**: 内存不足

```bash
# 解决方案：分批处理
# 可以按日期范围分批爬取
```

### 日志文件位置

```
logs/
├── crawler_YYYYMMDD_HHMMSS.log    # 主日志文件
└── error_YYYYMMDD_HHMMSS.log      # 错误日志 (如果有)
```

## 🔧 开发说明

### 项目结构

```
JobCrawler/
├── jobsdb_crawler.py          # 主爬虫逻辑
├── jobsdb_job_details.py      # 职位详情获取
├── logger_config.py           # 日志配置
├── requirements.txt           # 依赖列表
├── README.md                  # 本文件
├── data/                      # 数据输出目录
├── logs/                      # 日志目录
└── sample/                    # 示例文件
```

### 核心类说明

- **JobsDBCrawler**: 主爬虫类，控制整个爬取流程
- **CrawlerConfig**: 配置类，管理所有可调参数
- **PseudoThreadedCrawler**: 伪线程类，模拟多线程用户配置

### 扩展开发

```python
# 添加新的数据字段
def extract_single_job(self, data, job_id):
    # 在这里添加新字段的提取逻辑
    return {
        'job_id': job_id,
        'new_field': data.get('path', {}).get('to', {}).get('new_field', ''),
        # ... 其他字段
    }
```

## 📄 许可证

本项目仅供学习和研究使用，请遵守相关网站的使用条款和 robots.txt 规则。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进本项目。

## 📞 联系

如有问题或建议，请通过 Issue 联系。

---

**注意**: 使用本工具时请遵守目标网站的使用条款，合理控制爬取频率，避免对服务器造成过大压力。
