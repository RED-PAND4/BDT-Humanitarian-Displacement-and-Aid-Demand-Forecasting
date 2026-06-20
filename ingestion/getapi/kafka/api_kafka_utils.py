import json
import os
import sys
from typing import Any, Dict, Optional, List
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import requests
from confluent_kafka import Producer

try:
    from kafka.kafka_utils import ensure_topics
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kafka.kafka_utils import ensure_topics


HAPI_PREFIX = "https://hapi.humdata.org/api/v2"
APP_IDENTIFIER = "YmR0LUhEQUQ6YS5kZWJvcmFoLmluZkBnbWFpbC5jb20="

DEFAULT_API = " https://hapi.humdata.org/api/v2/affected-people/humanitarian-needs?app_identifier=YmR0LUhEQUQ6YS5kZWJvcmFoLmluZkBnbWFpbC5jb20="


def get_kafka_producer() -> Producer:
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return Producer({"bootstrap.servers": bootstrap_servers})

def add_hapi_app_identifier(url: str) -> str:
    """
    If the URL belongs to the HAPI API, ensure that
    app_identifier is present in the query string.
    """

    if not url.startswith(HAPI_PREFIX):
        return url

    parsed = urlparse(url)

    query = dict(parse_qsl(parsed.query))

    # Don't add it twice
    if "app_identifier" not in query:
        query["app_identifier"] = APP_IDENTIFIER

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query),
            parsed.fragment,
        )
    )


def fetch_data_api(endpoint: Optional[str] = None, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = endpoint or os.environ.get("API_URL", DEFAULT_API)
    url = add_hapi_app_identifier(url)
    response = requests.get(url, params=params or {})
    #print(response.json())
    response.raise_for_status()
    return response.json()


def publish_to_kafka(topic: str, payload: Dict[str, Any]) -> None:
    producer = get_kafka_producer()
    payload_bytes = json.dumps(payload).encode("utf-8")

    def delivery_report(err, msg):
        if err is not None:
            raise RuntimeError(f"Kafka publish failed: {err}")

    producer.produce(topic, payload_bytes, callback=delivery_report)
    producer.flush(10)


def extract_records(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Find and return the list of records from an API response.
    """

    candidate_fields = ["items", "results", "data"]

    for field in candidate_fields:
        if field in response and isinstance(response[field], list):
            return response[field]

    for key, value in response.items():
        if (
            isinstance(value, list)
            and len(value) > 0
            and isinstance(value[0], dict)
        ):
            return value

    raise ValueError(
        f"Could not find a record collection. Available keys: {list(response.keys())}"
    )


def fetch_and_publish(topic: Optional[str] = None,endpoint: Optional[str] = None,params: Optional[Dict[str, Any]] = None,) -> Dict[str, Any]:

    topic_name = topic or os.environ.get("API_TOPIC", "tester")

    ensure_topics([
        {"name": topic_name, "partitions": 1, "replication": 1},
    ])

    response = fetch_data_api(
        endpoint=endpoint,
        params=params
    )

    records = extract_records(response)
    #print(records)

    total_size = 0

    for record in records:
        print("----------------record----------------")
        print(record)
        print("----------------record----------------")
        publish_to_kafka(topic_name, record)
        total_size += len(json.dumps(record))

    return {
        "topic": topic_name,
        "records_published": len(records),
        "total_size": total_size,
    }