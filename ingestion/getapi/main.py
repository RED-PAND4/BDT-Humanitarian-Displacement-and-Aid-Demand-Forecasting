import sys
import os
try:
    from kafka.api_kafka_utils import etch_and_publish, fetch_data_api
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api

def main() -> None:
    #result = fetch_and_publish(topic="test_data_v3", endpoint="https://api.unhcr.org/population/v1/countries/", params={"limit": 500})
    #print(result)
    # fetch_and_publish(topic="currency", endpoint="https://hapi.humdata.org/api/v2/metadata/currency")
    # fetch_and_publish(topic="food_prices_market_monitor", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor")
    # fetch_and_publish(topic="food_security", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security")
    # fetch_and_publish(topic="location", endpoint="https://hapi.humdata.org/api/v2/metadata/location")
    # fetch_and_publish(topic="org_type", endpoint="https://hapi.humdata.org/api/v2/metadata/org-type")
    # fetch_and_publish(topic="org", endpoint="https://hapi.humdata.org/api/v2/metadata/org")
    # fetch_and_publish(topic="poverty_rate", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate")
    # fetch_and_publish(topic="sector", endpoint="https://hapi.humdata.org/api/v2/metadata/sector")
    # fetch_and_publish(topic="wfp_commodity", endpoint="https://hapi.humdata.org/api/v2/metadata/wfp-commodity")
    # fetch_and_publish(topic="wfp_market", endpoint="https://hapi.humdata.org/api/v2/metadata/wfp-market")

    # result1 = fetch_and_publish(topic="currency", endpoint="https://hapi.humdata.org/api/v2/metadata/currency")
    # print(result1)
    # result2 = fetch_and_publish(topic="food_prices_market_monitor", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor")
    # print(result2)
    # result3 = fetch_and_publish(topic="food_security", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security")
    # print(result3)
    # result4 = fetch_and_publish(topic="location", endpoint="https://hapi.humdata.org/api/v2/metadata/location")
    # print(result4)
    # result5 = fetch_and_publish(topic="org_type", endpoint="https://hapi.humdata.org/api/v2/metadata/org-type")
    # print(result5)
    # result6 = fetch_and_publish(topic="org", endpoint="https://hapi.humdata.org/api/v2/metadata/org")
    # print(result6)
    # result7 = fetch_and_publish(topic="poverty_rate", endpoint="https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate")
    # print(result7)
    # result8 = fetch_and_publish(topic="sector", endpoint="https://hapi.humdata.org/api/v2/metadata/sector")
    # print(result8)
    # result9 = fetch_and_publish(topic="wfp_commodity", endpoint="https://hapi.humdata.org/api/v2/metadata/wfp-commodity")
    # print(result9)
    # result10 = fetch_and_publish(topic="wfp_market", endpoint="https://hapi.humdata.org/api/v2/metadata/wfp-market")
    # print(result10)

    # Lista delle API con il loro stato e offset iniziale
    api_list = [
        {"topic": "currency", "endpoint": "https://hapi.humdata.org/api/v2/metadata/currency", "offset": 0, "active": True},
        {"topic": "food_prices_market_monitor", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor", "offset": 0, "active": True},
        {"topic": "food_security", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security", "offset": 0, "active": True},
        {"topic": "location", "endpoint": "https://hapi.humdata.org/api/v2/metadata/location", "offset": 0, "active": True},
        {"topic": "org_type", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org-type", "offset": 0, "active": True},
        {"topic": "org", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org", "offset": 0, "active": True},
        {"topic": "poverty_rate", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate", "offset": 0, "active": True},
        {"topic": "sector", "endpoint": "https://hapi.humdata.org/api/v2/metadata/sector", "offset": 0, "active": True},
        {"topic": "wfp_commodity", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-commodity", "offset": 0, "active": True},
        {"topic": "wfp_market", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-market", "offset": 0, "active": True},
        {"topic": "idps", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/idps/", "offset": 0, "active": True},
        {"topic": "humanitarian_needs", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/humanitarian-needs/", "offset": 0, "active": True},
        {"topic": "population", "endpoint": "https://api.unhcr.org/population/v1/population/", "offset": 0, "active": True},
        {"topic": "solutions", "endpoint": "https://api.unhcr.org/population/v1/solutions/", "offset": 0, "active": True}
    ]


    LIMIT = 1000

    # --- CONFIGURAZIONE DI SICUREZZA ---
    MAX_ITERATIONS = 5  # Imposta qui quante volte al massimo il ciclo intero può girare
    iteration_count = 0
    forced_stop = False

    # Il ciclo continua finché c'è un'API attiva E non abbiamo superato il limite di iterazioni
    while any(api["active"] for api in api_list):
        iteration_count += 1
        
        # Controllo di sicurezza: interrompi se abbiamo fatto troppi giri
        if iteration_count > MAX_ITERATIONS:
            print(f"\n⚠️ ATTENZIONE: Raggiunto il limite massimo di {MAX_ITERATIONS} iterazioni. Blocco di sicurezza attivato.")
            forced_stop = True
            break
            
        print(f"=== INIZIO ITERAZIONE DI GRUPPO N. {iteration_count} ===")
        
        for api in api_list:
            if not api["active"]:
                continue
                
            print(f"Estrazione in corso: {api['topic']} (Offset: {api['offset']})")
            
            current_params = {
                "limit": LIMIT,
                "offset": api["offset"]
            }
            
            # Chiamata alla tua funzione
            result = fetch_and_publish(
                topic=api["topic"], 
                endpoint=api["endpoint"], 
                params=current_params
            )
            
            num_records = result.get("records_published", 0) if result else 0
            print(f"-> Pubblicati {num_records} record per {api['topic']}.")
            
            if num_records < LIMIT:
                print(f"✓ {api['topic']} ha terminato tutti i dati. Disattivazione.\n")
                api["active"] = False
            else:
                api["offset"] += LIMIT
                print(f"↻ {api['topic']} continua al prossimo ciclo.\n")

    # Messaggio finale differenziato a seconda di come è terminato il ciclo
    if forced_stop:
        print("Processo interrotto prematuramente dal limite di sicurezza.")
    else:
        print("Tutte le API hanno terminato l'estrazione con successo (entro i limiti stabiliti).")


if __name__ == "__main__":
    main()

