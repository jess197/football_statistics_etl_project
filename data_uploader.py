from azure.storage.blob import BlobServiceClient
import os 

class DataUploader:
    
    def __init__(self):
        connection_string = os.getenv('CONN_STRING')
        self.container_name = 'footballapi-data'
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)


    def upload_file_to_BlobStorage(self, file_name,file):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name,blob=file_name)
        try:
            blob_client.upload_blob(file,overwrite=True)
            print(f"Uploaded {file_name} with success")
        except Exception as e:
            print("An error occurred during the upload of the file to Azure Blob Storage: ")
            print(str(e))

        
    def blob_exists(self,file_name):   
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name,blob=file_name)
        return blob_client.exists()