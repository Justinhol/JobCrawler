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


def get_job_details(job_id, threaded_crawler):
    """获取职位详情（移除重试机制）"""
    url = "https://hk.jobsdb.com/graphql"
    try:
        # 使用线程当前配置
        user_info = threaded_crawler.get_thread_user_info()

        # 使用新配置生成请求头
        headers = {
            "Cookie": user_info['cookie'],
            "User-Agent": user_info['user_agent'],
            "X-Seek-EC-SessionId": user_info['session_id'],
            "X-Seek-EC-VisitorId": user_info['session_id']
        }

        variables = {
            "sessionId": user_info['session_id'],  # 使用线程独立session_id
            "jobId": str(job_id),
            "jobDetailsViewedCorrelationId": "3c9373ae-c375-40c7-9c33-2c57253ea739",
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

        response = requests.post(url, json=payload, headers=headers)

        # 保留频率限制检查
        if response.status_code == 429:
            wait_time = 60 + random.uniform(10, 30)
            print(f"请求频率过高，等待{wait_time:.2f}秒后重试...")
            time.sleep(wait_time)
            return None  # 返回空让主流程处理

        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"请求失败: {str(e)}")
        return None


if __name__ == "__main__":
    # 测试获取指定职位信息
    from jobsdb_crawler import JobsDBCrawler
    crawler = JobsDBCrawler()
    job_id = "81468358"
    data = get_job_details(job_id, crawler)
    print(data)
