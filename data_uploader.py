#from azure.storage.blob import BlobServiceClient
import boto3
import os


class DataUploader:
    
    def __init__(self):
       # connection_string = os.getenv('CONN_STRING')
       # self.container_name = 'footballapi-data'
       # self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.bucket_name = 'football-data-bucket-s3'
        self.aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID') 
        self.aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')


    def upload_file_to_S3(self, file_name, file):
        s3 = boto3.client('s3', aws_access_key_id=self.aws_access_key_id,
                        aws_secret_access_key=self.aws_secret_access_key)
        try:
            s3.put_object(
                Bucket=self.bucket_name,
                Key=file_name,
                Body=file
            )
            print(f"Uploaded {file_name} to Amazon S3 with success")
        except Exception as e:
            print("An error occurred during the upload of the file to Amazon S3:")
            print(str(e))

    def blob_exists(self, file_name):
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=file_name)
        return 'Contents' in response

    '''    
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
    '''

    


