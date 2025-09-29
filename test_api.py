import requests
import json

# URL del endpoint
url = "http://localhost:8000/api/iterative"

# Datos de la petición
data = {
    "question": "Cuantas citas hay en el 2025",
    "llm_provider": "deepseek",
    "max_iterations": 5
}

# Headers
headers = {
    "accept": "application/json",
    "Content-Type": "application/json"
}

try:
    # Hacer la petición
    response = requests.post(url, json=data, headers=headers)
    
    # Mostrar el resultado
    print(f"Status Code: {response.status_code}")
    print(f"Response:")
    print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    
except Exception as e:
    print(f"Error: {e}")
