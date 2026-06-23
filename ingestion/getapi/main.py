import sys
import os
try:
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api
except ImportError:
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    from kafka.api_kafka_utils import fetch_and_publish, fetch_data_api

def main() -> None:
    
    # Lista delle API con il loro stato e offset iniziale
    # api_list = [
    #     {"topic": "currency", "endpoint": "https://hapi.humdata.org/api/v2/metadata/currency", "offset": 0, "active": True},
    #     {"topic": "food_prices_market_monitor", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor", "offset": 0, "active": True},
    #     {"topic": "food_security", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security", "offset": 0, "active": True},
    #     {"topic": "location", "endpoint": "https://hapi.humdata.org/api/v2/metadata/location", "offset": 0, "active": True},
    #     {"topic": "org_type", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org-type", "offset": 0, "active": True},
    #     {"topic": "org", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org", "offset": 0, "active": True},
    #     {"topic": "poverty_rate", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate", "offset": 0, "active": True},
    #     {"topic": "sector", "endpoint": "https://hapi.humdata.org/api/v2/metadata/sector", "offset": 0, "active": True},
    #     {"topic": "wfp_commodity", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-commodity", "offset": 0, "active": True},
    #     {"topic": "wfp_market", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-market", "offset": 0, "active": True}
    # ]

    # LIMIT = 1000

    # # --- CONFIGURAZIONE DI SICUREZZA ---
    # MAX_ITERATIONS = 5  # Imposta qui quante volte al massimo il ciclo intero può girare
    # iteration_count = 0
    # forced_stop = False

    # # Il ciclo continua finché c'è un'API attiva E non abbiamo superato il limite di iterazioni
    # while any(api["active"] for api in api_list):
    #     iteration_count += 1
        
    #     # Controllo di sicurezza: interrompi se abbiamo fatto troppi giri
    #     if iteration_count > MAX_ITERATIONS:
    #         print(f"\n⚠️ ATTENZIONE: Raggiunto il limite massimo di {MAX_ITERATIONS} iterazioni. Blocco di sicurezza attivato.")
    #         forced_stop = True
    #         break
            
    #     print(f"=== INIZIO ITERAZIONE DI GRUPPO N. {iteration_count} ===")
        
    #     for api in api_list:
    #         if not api["active"]:
    #             continue
                
    #         print(f"Estrazione in corso: {api['topic']} (Offset: {api['offset']})")
            
    #         current_params = {
    #             "limit": LIMIT,
    #             "offset": api["offset"]
    #         }
            
    #         # Chiamata alla tua funzione
    #         result = fetch_and_publish(
    #             topic=api["topic"], 
    #             endpoint=api["endpoint"], 
    #             params=current_params
    #         )
            
    #         num_records = result.get("records_published", 0) if result else 0
    #         print(f"-> Pubblicati {num_records} record per {api['topic']}.")
            
    #         if num_records < LIMIT:
    #             print(f"✓ {api['topic']} ha terminato tutti i dati. Disattivazione.\n")
    #             api["active"] = False
    #         else:
    #             api["offset"] += LIMIT
    #             print(f"↻ {api['topic']} continua al prossimo ciclo.\n")

    # # Messaggio finale differenziato a seconda di come è terminato il ciclo
    # if forced_stop:
    #     print("Processo interrotto prematuramente dal limite di sicurezza.")
    # else:
    #     print("Tutte le API hanno terminato l'estrazione con successo (entro i limiti stabiliti).")

    # --- API a OFFSET (HumData) ---
    api_list = [
        {"topic": "currency", "endpoint": "https://hapi.humdata.org/api/v2/metadata/currency", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "food_prices_market_monitor", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-prices-market-monitor", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "food_security", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "location", "endpoint": "https://hapi.humdata.org/api/v2/metadata/location", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "org_type", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org-type", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "org", "endpoint": "https://hapi.humdata.org/api/v2/metadata/org", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "poverty_rate", "endpoint": "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/poverty-rate", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "sector", "endpoint": "https://hapi.humdata.org/api/v2/metadata/sector", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "wfp_commodity", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-commodity", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "wfp_market", "endpoint": "https://hapi.humdata.org/api/v2/metadata/wfp-market", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "conflict_events", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/conflict-events", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "funding", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/funding", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "national_risk", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/national-risk", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "operational_presence", "endpoint": "https://hapi.humdata.org/api/v2/coordination-context/operational-presence", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "idps", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/idps/", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "baseline_population", "endpoint": "https://hapi.humdata.org/api/v2/geography-infrastructure/baseline-population", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "humanitarian_needs", "endpoint": "https://hapi.humdata.org/api/v2/affected-people/humanitarian-needs/", "pagination_type": "offset", "current": 0, "limit": 1000, "active": True},
        {"topic": "population", "endpoint": "https://api.unhcr.org/population/v1/population/", "pagination_type": "page", "current": 1, "limit": 500, "active": True},
        {"topic": "solutions", "endpoint": "https://api.unhcr.org/population/v1/solutions/", "pagination_type": "page", "current": 1, "limit": 500, "active": True}
    ]

    MAX_ITERATIONS = 3  # Sicurezza per evitare cicli infiniti durante i test
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
            current_params = {"limit": api["limit"]}
            
            if api["pagination_type"] == "offset":
                current_params["offset"] = api["current"]
                print(f"Estrazione {api['topic']} -> Tipo: Offset, Valore: {api['current']}")
                
            elif api["pagination_type"] == "page":
                current_params["page"] = api["current"]
                print(f"Estrazione {api['topic']} -> Tipo: Pagina, Valore: {api['current']}")
            
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
                elif api["pagination_type"] == "page":
                    api["current"] += 1             # Le pagine aumentano di 1, 2, 3...
                    
                print(f"↻ {api['topic']} ha ancora dati. Prossimo valore: {api['current']}")

print("\nFine del processo.")


if __name__ == "__main__":
    main()

