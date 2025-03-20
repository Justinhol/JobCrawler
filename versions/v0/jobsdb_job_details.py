import requests
import json
from datetime import datetime
from typing import Dict, Any, Optional
import time
import random


def safe_get(data: Optional[Dict[str, Any]], *keys: str, default: Any = "未提供") -> Any:
    """安全地获取嵌套字典中的值"""
    current = data
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def get_job_details(job_id, crawler_instance, max_retries=3, user_index=None):
    """获取职位详情，添加重试机制"""
    url = "https://hk.jobsdb.com/graphql"

    # 获取当前用户信息
    if user_index is not None:
        user_info = {
            'cookie': crawler_instance.cookies[user_index],
            'session_id': crawler_instance.session_ids[user_index],
            'user_agent': crawler_instance.user_agents[user_index]
        }
    else:
        user_info = crawler_instance.get_user_info()

    session_id = user_info['session_id']

    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9",
        "Content-Type": "application/json",
        "Cookie": user_info['cookie'],
        "User-Agent": user_info['user_agent'],
        "Origin": "https://hk.jobsdb.com",
        "Referer": f"https://hk.jobsdb.com/jobs?classification=6123%2C6281&jobId={job_id}&type=standard",
        "seek-request-brand": "jobsdb",
        "seek-request-country": "HK",
        "X-Seek-EC-SessionId": session_id,
        "X-Seek-EC-VisitorId": session_id,
        "X-Seek-Site": "chalice"
    }

    # 构建GraphQL查询参数
    variables = {
        "jobId": str(job_id),
        "jobDetailsViewedCorrelationId": "3c9373ae-c375-40c7-9c33-2c57253ea739",
        "sessionId": session_id,
        "zone": "asia-1",
        "locale": "en-HK",
        "languageCode": "en",
        "timezone": "Asia/Hong_Kong"
    }

    # GraphQL查询
    query = """
    query jobDetails($jobId: ID!, $jobDetailsViewedCorrelationId: String!, $sessionId: String!, $zone: Zone!, $locale: Locale!, $languageCode: LanguageCodeIso!, $timezone: Timezone!) {
      jobDetails(
        id: $jobId
        tracking: {channel: "WEB", jobDetailsViewedCorrelationId: $jobDetailsViewedCorrelationId, sessionId: $sessionId}
      ) {
        job {
          sourceZone
          tracking {
            adProductType
            classificationInfo {
              classificationId
              classification
              subClassificationId
              subClassification
              __typename
            }
            hasRoleRequirements
            isPrivateAdvertiser
            locationInfo {
              area
              location
              locationIds
              __typename
            }
            workTypeIds
            postedTime
            __typename
          }
          id
          title
          phoneNumber
          isExpired
          expiresAt {
            dateTimeUtc
            __typename
          }
          isLinkOut
          contactMatches {
            type
            value
            __typename
          }
          isVerified
          abstract
          content(platform: WEB)
          status
          listedAt {
            label(context: JOB_POSTED, length: SHORT, timezone: $timezone, locale: $locale)
            dateTimeUtc
            __typename
          }
          salary {
            currencyLabel(zone: $zone)
            label
            __typename
          }
          shareLink(platform: WEB, zone: $zone, locale: $locale)
          workTypes {
            label(locale: $locale)
            __typename
          }
          advertiser {
            id
            name(locale: $locale)
            isVerified
            registrationDate {
              dateTimeUtc
              __typename
            }
            __typename
          }
          location {
            label(locale: $locale, type: LONG)
            __typename
          }
          classifications {
            label(languageCode: $languageCode)
            __typename
          }
          products {
            branding {
              id
              cover {
                url
                __typename
              }
              thumbnailCover: cover(isThumbnail: true) {
                url
                __typename
              }
              logo {
                url
                __typename
              }
              __typename
            }
            bullets
            questionnaire {
              questions
              __typename
            }
            video {
              url
              position
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
    }
    """

    payload = {
        "operationName": "jobDetails",
        "variables": variables,
        "query": query
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, headers=headers)

            # 检查是否触发了频率限制
            if response.status_code == 429:
                wait_time = 60 + random.uniform(10, 30)  # 基础等待时间60秒，加上随机等待
                print(f"请求频率过高，等待{wait_time:.2f}秒后重试...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            data = response.json()

            # 检查返回的数据中是否包含频率限制错误
            if 'errors' in data and any('RATE_LIMITED' in error.get('extensions', {}).get('code', '') for error in data['errors']):
                wait_time = 60 + random.uniform(10, 30)
                print(f"API频率限制，等待{wait_time:.2f}秒后重试...")
                time.sleep(wait_time)
                continue

            return data

        except requests.RequestException as e:
            print(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"错误详情: {e.response.text}")

            # 如果不是最后一次尝试，则等待后重试
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 30 + \
                    random.uniform(5, 15)  # 递增等待时间
                print(f"等待{wait_time:.2f}秒后重试...")
                time.sleep(wait_time)
            else:
                print("已达到最大重试次数，放弃请求")
                return None

    return None


if __name__ == "__main__":
    # 测试获取指定职位信息
    from jobsdb_crawler import JobsDBCrawler
    crawler = JobsDBCrawler()
    job_id = "81468358"
    data = get_job_details(job_id, crawler)
    print(data)
