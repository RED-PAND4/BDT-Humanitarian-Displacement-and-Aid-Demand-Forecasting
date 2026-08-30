import sys
import os
import argparse
try:
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api


def main() -> None:

    # 1. Set up the argument parser
    parser = argparse.ArgumentParser(description="Fetch API data for a range of years.")
    parser.add_argument("--year_from", type=int, required=True, help="The starting year")
    parser.add_argument("--year_to", type=int, required=True, help="The ending year")
    
    # 2. Parse the arguments coming from Airflow
    args = parser.parse_args()
    
    # 3. Use them in your code as native integers
    start_year = args.year_from
    end_year = args.year_to
    
    print(f"Starting API extraction from {start_year} to {end_year}...")
    
   
    #--- API a OFFSET (HumData) ---
    api_list = [
        # POPULATION (UNHCR API): refugees, asylum_seekers, idps, oip, occ, stateless, hst by coo/coa/year
        {"topic": "population", "endpoint": "https://api.unhcr.org/population/v1/population/", "pagination_type": "page", "current": 1, "limit": 10000, "active": True},
        # SOLUTIONS (UNHCR API): returned_refugees, returned_idps, resettlement, naturalisation by coo/coa/year
        {"topic": "solutions", "endpoint": "https://api.unhcr.org/population/v1/solutions/", "pagination_type": "page", "current": 1, "limit": 10000, "active": True},   
        # NOWCASTING (UNHCR API): mid-year estimates
        {"topic": "nowcasting", "endpoint": "https://api.unhcr.org/population/v1/nowcasting/", "pagination_type": "page", "current": 1, "limit": 10000, "active": True},
        # UNRWA (UNHCR API): Palestinian refugees registered under the UNRWA mandate
        {"topic": "unrwa", "endpoint": "https://api.unhcr.org/population/v1/unrwa/", "pagination_type": "page", "current": 1, "limit": 10000, "active": True},

        # Total Population (World Bank API): total population by country/year
        {"topic": "worldbank_population", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True},
        # GDP per capita (World Bank API): GDP per capita by country/year
        {"topic": "worldbank_gdp", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True}, 

        # Conflict Events (HDX HAPI)
        {"topic": "conflict_events", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/conflict-events", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},

        # Poverty Rate MPI (HDX HAPI)
        {"topic": "poverty_rate", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # Multidimensional poverty headcount ratio MPM (World Bank API) (% of population)
        {"topic": "worldbank_mpi", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/SI.POV.MPWB", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True},
        # Poverty headcount ratio at $3.00 a day (2021 PPP) (% of population)
        {"topic": "worldbank_extreme_poverty", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/SI.POV.DDAY", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True},

        # Food Security (HDX HAPI)
        {"topic": "food_security", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},

        # Funding (HDX HAPI)
        {"topic": "funding", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/funding", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},

        # IDPs (HDX HAPI): idps by coo/coa/year
        {"topic": "idps", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/idps/", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # Humanitarian Needs (HDX HAPI)
        {"topic": "humanitarian_needs", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/humanitarian-needs/", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # Operational Presence (HDX HAPI)
        {"topic": "operational_presence", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/operational-presence", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # Sector metadata (HDX HAPI): each sector with its code and name
        {"topic": "sector", "endpoint": "https://hapi.humdata.org/api/v2/metadata/sector", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        
        # Location metadata (HDX HAPI): each country with its flag has_hrp and in_gho (from which year)
        {"topic": "location", "endpoint": "https://hapi.humdata.org/api/v2/metadata/location", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # National Risk (HDX HAPI)
        {"topic": "national_risk", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/national-risk", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True}
                    
    ]

    MAX_ITERATIONS = 5  # Security to avoid infinite loops during testing
    iteration_count = 0

    while any(api["active"] for api in api_list):
        iteration_count += 1
        if iteration_count > MAX_ITERATIONS:
            print(f"\n⚠️ Security block: reached the limit of {MAX_ITERATIONS} iterations.")
            break
            
        print(f"\n=== GROUP ITERATION N. {iteration_count} ===")
        
        for api in api_list:
            if not api["active"]:
                continue
                
            # 1. We build dynamic parameters based on pagination type
            current_params = {}

            # Pagination style for the HDX HAPI
            if api["pagination_type"] == "offset":
                current_params["limit"] = api["limit"]
                current_params["offset"] = api["current"]
                current_params["start_date"] = str(start_year)
                current_params["end_date"] = str(end_year)
                print(f"Extraction {api['topic']} -> Type: Offset, Value: {api['current']}")

            # Pagination style for the UNHCR API
            elif api["pagination_type"] == "page":
                current_params["limit"] = api["limit"]
                current_params["page"] = api["current"]
                current_params["coa_all"] = True
                current_params["coo_all"] = True
                current_params["yearFrom"] = start_year
                current_params["yearTo"] = end_year
                print(f"Extraction {api['topic']} -> Type: Page, Value: {api['current']}")

            # Pagination style for the World Bank API
            elif api["pagination_type"] == "page_wb":
                current_params["per_page"] = api["limit"]
                current_params["page"] = api["current"]
                current_params["format"] = "json"
                # World Bank Syntax for years: YYYY:YYYY
                current_params["date"] = f"{start_year}:{end_year}"
                print(f"Extraction {api['topic']} -> Type: Page_WB, Value: {api['current']}")
            
            # 2. Let's execute the API call
            result = fetch_and_publish(
                topic=api["topic"], 
                endpoint=api["endpoint"], 
                params=current_params
            )
            
            num_records = result.get("records_published", 0) if result else 0
            print(f"-> Received and published {num_records} records.")
            
            # 3. Let's check if the data is finished
            if num_records < api["limit"]:
                print(f"✓ {api['topic']} has no more data. Deactivated.")
                api["active"] = False
            else:
                # 4. Let's increment the counter in a specific way for the API type
                if api["pagination_type"] == "offset":
                    api["current"] += api["limit"]  # The offset increases by 1000, 2000, 3000...
                elif api["pagination_type"] in ["page", "page_wb"]:
                    api["current"] += 1
                    
                print(f"↻ {api['topic']} has more data. Next value: {api['current']}")

    print("\nEnd of API extraction process.")


if __name__ == "__main__":
    main()

