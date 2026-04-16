from google.cloud import bigquery

def main():
    client = bigquery.Client(project='prueba-tecnica-ceiba-software')
    print(client)
    


if __name__ == "__main__":
    main()
