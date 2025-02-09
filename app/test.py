# import requests

# # URL вашего FastAPI-сервера (замените, если сервер на другом хосте)
# # API_URL = "http://127.0.0.1:8000/unipro/data"
# API_URL = "http://127.0.0.1:8000/developer_panel/developer_panel/unipro/data"

# # Тестовые данные, имитирующие ответ от Unipro
# test_data = {
#     "info": {
#         "source": "Unipro",
#         "date": "2024-02-07"
#     },
#     "goods": [
#         {"code": "123", "name": "Товар 1", "price": 100.0},
#         {"code": "456", "name": "Товар 2", "price": 200.0}
#     ]
# }

# # Отправка POST-запроса на ваш API
# response = requests.post(API_URL, json=test_data)

# # Вывод ответа API
# print("📌 Ответ сервера:")
# print(response, response.text, response.status_code)
