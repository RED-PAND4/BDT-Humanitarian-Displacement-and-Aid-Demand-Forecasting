try:
    from kafka.api_kafka_utils import etch_and_publish, fetch_data_api
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api

def main() -> None:
    result = fetch_and_publish(topic="test_data_v3", endpoint="https://api.unhcr.org/population/v1/countries/", params={"limit": 500})
    print(result)


if __name__ == "__main__":
    main()

