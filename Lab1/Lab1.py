"""
FILE: lab1.py
DESCRIPTION:
    Lab 1
USAGE:
    python lab1.py
    Set the environment variables with your own values before running the sample:
    1) STORAGE_ACCOUNT_NAME - the storage account name
    2) STORAGE_ACCOUNT_KEY - the storage account key
"""

import os
import random
import pyarrow.parquet as pq
import zipfile

# Import the needed credential and management objects from the libraries.
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient

from dotenv import load_dotenv

# Chargez les variables d'environnement depuis le fichier .env
load_dotenv()

# Acquire a credential object using CLI-based authentication.
credential = AzureCliCredential()

# Retrieve subscription ID from environment variable.
subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

# Obtain the management object for resources.
resource_client = ResourceManagementClient(credential, subscription_id)

storage_client = StorageManagementClient(credential, subscription_id)

from azure.storage.filedatalake import (
    DataLakeServiceClient,
)

def create_rg():

    rg_name =  f"PythonAzureExample-rg-{random.randint(1,100000):05}"

    # Provision the resource group.
    rg_result = resource_client.resource_groups.create_or_update(
        rg_name, {"location": "centralus"}
    )

    print(
        f"Provisioned resource group {rg_result.name} in \
            the {rg_result.location} region"
    )

    return rg_result
        
def create_storage():

    rg = create_rg()

    RESOURCE_GROUP_NAME = rg.name
    LOCATION = rg.location
    STORAGE_ACCOUNT_NAME = f"pythonazurestorage{random.randint(1,100000):05}"

    availability_result = storage_client.storage_accounts.check_name_availability(
        { "name": STORAGE_ACCOUNT_NAME }
    )

    if not availability_result.name_available:
        print(f"Storage name {STORAGE_ACCOUNT_NAME} is already in use. Try another name.")
        exit()

    # The name is available, so provision the account
    poller = storage_client.storage_accounts.begin_create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,
        {
            "location" : LOCATION,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"}
        }
    )

    # Long-running operations return a poller object; calling poller.result()
    # waits for completion.
    account_result = poller.result()
    print(f"Provisioned storage account {account_result.name}")


    # Step 3: Retrieve the account's primary access key and generate a connection string.
    keys = storage_client.storage_accounts.list_keys(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)

    print(f"Primary key for storage account: {keys.keys[0].value}")

    conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"

    print(f"Connection string: {conn_string}")

    # Step 4: Provision the blob container in the account (this call is synchronous)
    CONTAINER_NAME = f"blob-container-{random.randint(1,100000):05}"
    container = storage_client.blob_containers.create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME, CONTAINER_NAME, {})

    # The fourth argument is a required BlobContainer object, but because we don't need any
    # special values there, so we just pass empty JSON.

    print(f"Provisioned blob container {container.name}")

    return STORAGE_ACCOUNT_NAME, keys.keys[0].value, container.name

def upload_download_sample(filesystem_client, fichier, contenu):
    # create a file before writing content to it
    file_name = fichier
    print("Creating a file named '{}'.".format(file_name))
    # [START create_file]
    file_client = filesystem_client.get_file_client(file_name)
    file_client.create_file()
    # [END create_file]

    # prepare the file content with 4KB of random data
    file_content = contenu

    # append data to the file
    # the data remain uncommitted until flush is performed
    print("Uploading data to '{}'.".format(file_name))
    file_client.append_data(data=file_content, offset=0, length=len(file_content))

    # data is only committed when flush is called
    file_client.flush_data(len(file_content))

    # Get file properties
    # [START get_file_properties]
    properties = file_client.get_file_properties()
    # [END get_file_properties]

    # read the data back
    print("Downloading data from '{}'.".format(file_name))
    # [START read_file]
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    # [END read_file]

    # verify the downloaded content
    if file_content == downloaded_bytes:
        print("The downloaded data is equal to the data uploaded.")
    else:
        print("ok.")

    # Rename the file
    # [START rename_file]
    new_client = file_client#.rename_file(file_client.file_system_name + '/' + 'newname')
    # [END rename_file]

    # download the renamed file in to local file
    with open(fichier, 'wb') as stream:
        download = new_client.download_file()
        download.readinto(stream)

    # [START delete_file]
    #new_client.delete_file()
    # [END delete_file]


# help method to provide random bytes to serve as file content
def get_random_bytes(size):
    rand = random.Random()
    result = bytearray(size)
    for i in range(size):
        result[i] = int(rand.random()*255)  # random() is consistent between python 2 and 3
    return bytes(result)


def tmt(filesystem_client, fichier, contenu):

    # invoke the sample code
    try:
        upload_download_sample(filesystem_client, fichier, contenu)
    finally:
        # clean up the demo filesystem
        #filesystem_client.delete_file_system()
        print('end')


def detect_sous_dossiers(dossier_principal):
    # Initialiser une liste pour stocker les chemins des sous-dossiers
    chemins_sous_dossiers = []

    # Parcourir les sous-dossiers
    for dossier_racine, sous_dossiers, fichiers in os.walk(dossier_principal):
        for sous_dossier in sous_dossiers:
            chemin_sous_dossier = os.path.join(dossier_racine, sous_dossier)
            chemins_sous_dossiers.append(chemin_sous_dossier)

    # Afficher les chemins des sous-dossiers
    for chemin in chemins_sous_dossiers:
        print(f'Sous-dossier trouvé : {chemin}')

    return chemins_sous_dossiers


def upload_folder(dossier, filesystem_client):
     # Vérifie si le chemin est un dossier existant
    if os.path.isdir(dossier):
        # Liste tous les fichiers du dossier
        fichiers = os.listdir(dossier)

        for fichier in fichiers:
            chemin_fichier = os.path.join(dossier, fichier)
            base, extension = os.path.splitext(chemin_fichier)
            if extension == '.zip' :
                with zipfile.ZipFile(chemin_fichier, 'r') as zip_ref:
                    # Extraire tous les fichiers dans le dossier d'extraction
                    zip_ref.extractall(dossier)

        # Parcours tous les fichiers
        for fichier in fichiers:
            chemin_fichier = os.path.join(dossier, fichier)
            
            # Vérifie si le chemin est un fichier (et non un sous-dossier)
            if os.path.isfile(chemin_fichier):
                base, extension = os.path.splitext(chemin_fichier)
                print(extension)
                if extension == '.parquet' :
                    table = pq.read_table(chemin_fichier)
                    df = table.to_pandas()
                    # Affichage du DataFrame sous forme de chaîne de caractères
                    contenu = df.to_string()
                    tmt(filesystem_client, fichier, contenu)
                elif extension == '.zip' :
                    print('nothing to do')
                else :
                    with open(chemin_fichier, 'r') as source:
                    # Lire le contenu du fichier source
                        contenu = source.read()
                    tmt(filesystem_client, fichier, contenu)

            # Si vous souhaitez également parcourir les sous-dossiers, utilisez isdir au lieu de isfile
            # if os.path.isdir(chemin_fichier):
            #     print(f'Sous-dossier trouvé : {fichier}')
    else:
        print(f"Le dossier {dossier} n'existe pas.")


def run(dossier):

    account_name, account_key, container = create_storage()

    # set up the service client with the credentials from the environment variables
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key,)

    # create the filesystem
    filesystem_client = service_client.get_file_system_client(file_system=container)

    upload_folder(dossier, filesystem_client)

    sous_dossiers = detect_sous_dossiers(dossier)

    for sous_dossier in sous_dossiers:
        upload_folder(sous_dossier, filesystem_client) 


if __name__ == '__main__':
    dossier = 'data/'
    run(dossier)    
