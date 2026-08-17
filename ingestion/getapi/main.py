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
        # IDPs (HDX HAPI): idps by coo/coa/year
        {"topic": "idps", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/idps/", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        # SOLUTIONS (UNHCR API): returned_refugees, returned_idps, resettlement, naturalisation by coo/coa/year
        {"topic": "solutions", "endpoint": "https://api.unhcr.org/population/v1/solutions/", "pagination_type": "page", "current": 1, "limit": 10000, "active": True},   


        # Total Population (World Bank API): total population by country/year
        {"topic": "worldbank_population", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True},
        # GDP per capita (World Bank API): GDP per capita by country/year
        {"topic": "worldbank_gdp", "endpoint": "http://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD", "pagination_type": "page_wb", "current": 1, "limit": 10000, "active": True}, 

        # Conflict Events (HDX HAPI)
        {"topic": "conflict_events", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/conflict-events", "pagination_type": "offset", "current": 1800000, "limit": 10000, "active": True},

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

        # Humanitarian Needs (HDX HAPI)
        {"topic": "humanitarian_needs", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/humanitarian-needs/", "pagination_type": "offset", "current": 610000, "limit": 10000, "active": True},

        # Api per resource priotization (?)
        {"topic": "operational_presence", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/operational-presence", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "location", "endpoint": "https://hapi.humdata.org/api/v2/metadata/location", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "org_type", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org-type", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "org", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "sector", "endpoint": "https://hapi.humdata.org/api/v2/metadata/sector", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "wfp_commodity", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-commodity", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "wfp_market", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-market", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        
        # api inutili (?)
        {"topic": "currency", "endpoint": "https://hapi.humdata.org/api/v2/metadata/currency", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "food_prices_market_monitor", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor", "pagination_type": "offset", "current": 1800000, "limit": 10000, "active": True},
        {"topic": "national_risk", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/national-risk", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True},
        {"topic": "baseline_population", "endpoint": "https://hapi.humdata.org/api/v2/geography-infrastructure/baseline-population", "pagination_type": "offset", "current": 0, "limit": 10000, "active": True}
                

    ]

    MAX_ITERATIONS = 5  # Sicurezza per evitare cicli infiniti durante i test
    iteration_count = 0

    while any(api["active"] for api in api_list):
        iteration_count += 1
        if iteration_count > MAX_ITERATIONS:
            print(f"\n⚠️ Blocco di sicurezza: raggiunto il limite di {MAX_ITERATIONS} iterazioni.")
            break
            
        print(f"\n=== ITERAZIONE DI GRUPPO N. {iteration_count} ===")
        
        for api in api_list:
            if not api["active"]:
                continue
                
            # 1. Costruiamo i parametri dinamici in base al tipo di paginazione
            current_params = {}
            
            if api["pagination_type"] == "offset":
                current_params["limit"] = api["limit"]
                current_params["offset"] = api["current"]
                current_params["start_date"] = str(start_year)
                current_params["end_date"] = str(end_year)
                print(f"Estrazione {api['topic']} -> Tipo: Offset, Valore: {api['current']}")
                
            elif api["pagination_type"] == "page":
                current_params["limit"] = api["limit"]
                current_params["page"] = api["current"]
                current_params["coa_all"] = True
                current_params["coo_all"] = True
                current_params["yearFrom"] = start_year
                current_params["yearTo"] = end_year
                print(f"Estrazione {api['topic']} -> Tipo: Pagina, Valore: {api['current']}")

            elif api["pagination_type"] == "page_wb":
                current_params["per_page"] = api["limit"]
                current_params["page"] = api["current"]
                current_params["format"] = "json"
                # Sintassi World Bank per gli anni: YYYY:YYYY
                current_params["date"] = f"{start_year}:{end_year}"
                print(f"Estrazione {api['topic']} -> Tipo: Page_WB, Valore: {api['current']}")
            
            # 2. Eseguiamo la chiamata
            result = fetch_and_publish(
                topic=api["topic"], 
                endpoint=api["endpoint"], 
                params=current_params
            )
            
            num_records = result.get("records_published", 0) if result else 0
            print(f"-> Ricevuti e pubblicati {num_records} record.")
            
            # 3. Controlliamo se i dati sono finiti
            if num_records < api["limit"]:
                print(f"✓ {api['topic']} non ha più dati. Disattivata.")
                api["active"] = False
            else:
                # 4. Incrementiamo il contatore in modo specifico per il tipo di API
                if api["pagination_type"] == "offset":
                    api["current"] += api["limit"]  # L'offset aumenta di 1000, 2000, 3000...
                elif api["pagination_type"] in ["page", "page_wb"]:
                    api["current"] += 1
                    
                print(f"↻ {api['topic']} ha ancora dati. Prossimo valore: {api['current']}")

    print("\nFine del processo.")


if __name__ == "__main__":
    main()

