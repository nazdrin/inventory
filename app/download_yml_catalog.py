import requests

def download_yml(url: str, save_path: str = "catalog.xml"):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for HTTP errors

        with open(save_path, "wb") as f:
            f.write(response.content)
        
        print(f"Файл успешно сохранён как: {save_path}")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке файла: {e}")

if __name__ == "__main__":
    url = "https://my.foks.biz/s/pb/f?key=b269f35e-72d8-42b3-b8de-43ce9c65e7b9&type=yml_catalog&ext=xml"
    download_yml(url)
