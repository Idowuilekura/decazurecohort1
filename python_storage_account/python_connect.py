import os


from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient,
    DataLakeFileClient
)


from azure.identity import DefaultAzureCredential 

local_path = os.getcwd()

# sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-13T02:41:00Z&st=2024-08-02T18:41:00Z&sip=0.0.0.0&spr=https,http&sig=s9tiPJ923zKFcL6Vaeo1J7IkTB27J1OIHfT%2BZmGzJXw%3D"
# sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-03T02:47:12Z&st=2024-08-02T18:47:12Z&spr=https,http&sig=TepZxgjjTNPoZfraYdTMRe0%2FPQZiQTxKl1sm%2FcmX2ow%3D"
sas_token : str = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-08-03T04:24:57Z&st=2024-08-02T20:24:57Z&spr=https&sig=atmvWn4sVDsCyn461MuieC9ROkWr%2Ff7H3TATmTdLQCA%3D"

def get_service_client_sas(account_name: str, sas_token: str) -> DataLakeServiceClient:
    account_url = f"https://{account_name}.dfs.core.windows.net"

    # The SAS token string can be passed in as credential param or appended to the account URL
    service_client = DataLakeServiceClient(account_url, credential=sas_token)

    return service_client


try:
    print("about to connect to the storage account")
    service_client : DataLakeServiceClient = get_service_client_sas("decstg2", sas_token=sas_token)
except:
    print("error connecting to the service account")
    pass

def create_file_system(service_client: DataLakeServiceClient, file_system_name: str) -> FileSystemClient:
    
    file_system_client = service_client.create_file_system(file_system = file_system_name)

    return file_system_client

try:
    print("about to create the container")
    file_system_client : FileSystemClient = create_file_system(service_client=service_client, file_system_name="testingcontnewstg")
    print("done creating the container")
except:
    print("container already exist")
    pass

def create_directory(file_system_client: FileSystemClient, directory_name: str) -> DataLakeDirectoryClient:
    directory_client = file_system_client.create_directory(directory_name)

    return directory_client

try:
    print("about to create the directory")
    directory_client : DataLakeDirectoryClient = create_directory(file_system_client=file_system_client, directory_name="rawnew")
except:
    print("directory already exist")
# # get_service_client_sas("decstg2", sas_token=sas_token).create_file_system(file_system="hellofiledec").create_directory("raw")

def upload_file_to_directory( directory_client: DataLakeDirectoryClient, local_path: str, file_name: str):
    file_client = directory_client.get_file_client(file_name)

    with open(file=os.path.join(local_path, file_name), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)


# upload_file_to_directory(directory_client=directory_client, local_path=local_path, file_name="annual-enterprise-survey-2023-financial-year-provisional.csv")

# file = DataLakeFileClient.from_connection_string()
# service_client.get_directory_client()
dir_client = service_client.get_directory_client("testingcontnewstg","rawnew")

file_client = dir_client.get_file_client("annual-enterprise-survey-2023-financial-year-provisional.csv")
with open(file=os.path.join(local_path, "annual-enterprise-survey-2023-financial-year-provisional.csv"), mode="rb") as data:
        file_client.upload_data(data, overwrite=True)
