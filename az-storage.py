import os, uuid
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.mgmt.resource import ResourceManagementClient
import yaml 

root = os.getcwd()
path_config = "./config.yaml"

config_file = yaml.safe_load(open(path_config))


Storage_name = config_file["azure_config"]["Storage_name"]
Subscription_id = config_file["azure_config"]["Subscription_id"]
Key = config_file["azure_config"]["Key"]
Conn_string = config_file["azure_config"]["Conn_string"]
Container_name = config_file["azure_config"]["Container_name"]
blob_path = config_file["local_config"]["blob_path"]
RG_name = config_file["azure_config"]["RG_name"]

# Resource Groups
credentials = AzureCliCredential()

# Retrieve the resource group to use, defaulting to "myResourceGroup".
RG_name = os.getenv("RESOURCE_GROUP_NAME", RG_name)

# Create the BlobServiceClient object
blob_service_client = BlobServiceClient.from_connection_string(Conn_string)

#Create container
try:
    container_client = blob_service_client.get_container_client(Container_name)
    if not container_client.exists():
        blob_service_client.create_container(Container_name)
    else:
        print("Container Exists")

except Exception as ex:
    print("Exception:")
    print(ex)


container = ContainerClient.from_connection_string(conn_str=Conn_string, container_name=Container_name)
container_client = blob_service_client.get_container_client(Container_name)

blob_list = list(container.list_blobs())
blobs = []
for blob_name in blob_list:
    blobs.append(blob_name.name)

# Create a blob client using the local file name as the name for the blob
for local_file in os.listdir(blob_path):
    blob_client = blob_service_client.get_blob_client(container=Container_name, blob=local_file)
    
    if local_file in blobs:
        container_client.delete_blobs(local_file)

    upload_path = os.path.join(blob_path, local_file)
    with open(file=upload_path, mode="rb") as data:
        blob_client.upload_blob(data, blob_type="BlockBlob")
