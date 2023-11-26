from azure.storage.blob import BlobServiceClient, ContainerClient
import pandas as pd
from io import StringIO
import os

# Remplacez ces valeurs par les vôtres
account_name = "datalakestorageazure"
account_key = "dFxghcgXsiREsjzjCJIubh6b68LBlSsFBMCXMHJoH1Hh6YbnbBnXdebLEI0cfrCVQenwb3j7C/C7+AStD/SI+g=="
container_name = "data"

# Mapping entre company_name et stock_name
name_mapping = {
    'AMAZON': 'AMZN',
    'APPLE': 'AAPL',
    'FACEBOOK': 'FB',
    'GOOGLE': 'GOOG',
    'MICROSOFT': 'MSFT',
    'TESLA': 'TSLA',
    'ZOOM': 'ZM'
}

# Connexion au stockage Blob
blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
container_client = blob_service_client.get_container_client(container_name)

# Récupération de la liste des blobs dans le conteneur
blobs = container_client.list_blobs()

# Initialisation d'une liste pour stocker les DataFrames individuels
dataframes = []

# Boucle sur tous les blobs dans le conteneur
for blob in blobs:
    # Vérification si le blob est un fichier CSV
    if blob.name.endswith('.csv') and not blob.name.startswith('merged_data'):
        # Téléchargement du contenu du blob
        blob_data = container_client.get_blob_client(blob.name).download_blob().readall()

        # Conversion des données CSV en DataFrame
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))

        # Ajout de la colonne "stock_name" en utilisant le mapping
        df['stock_name'] = df['company_name'].map(name_mapping)

        # Ajout du DataFrame à la liste
        dataframes.append(df)

# Fusion des DataFrames
merged_df = pd.concat(dataframes, ignore_index=True)

# Spécification du chemin du fichier de sortie dans un autre dossier
output_file_path = "merged_data.csv"

# Enregistrement du DataFrame fusionné dans un nouveau fichier CSV
merged_df.to_csv(output_file_path, index=False)

# Upload du fichier CSV fusionné dans le conteneur Azure Blob
with open(output_file_path, "rb") as data:
    container_client.upload_blob(name=output_file_path, data=data, overwrite=True)

# Suppression du fichier local après l'upload
os.remove(output_file_path)

print(f"Le fichier CSV fusionné a été créé et enregistré dans {output_file_path}.")