import boto3
import gzip

def main():

    AccesKey='AKIAULWFHM3JKQGECW4Z' # replace with your access key
    SecretKey='jU4xyHKX9rfBNQEtuGwBcnsLemBnlTloH4ZEV7YX' #replace with you Secret Key
    BUCKET_NAME = 'commoncrawl' 
    KEY = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz' 
    
    
    s3 = boto3.resource('s3',aws_access_key_id=AccesKey,aws_secret_access_key=SecretKey)
    

    s3.Bucket(BUCKET_NAME).download_file(KEY, 'compfile.gz')
    
    with gzip.open('compfile.gz', 'rt', newline='\n') as f:
        file_content = f.readline()
        

    KEY = file_content.strip() 
    KEY = "crawl-data/CC-MAIN-2022-05/segments/1642320299852.23/wet/CC-MAIN-20220116093137-20220116123137-00000.warc.wet.gz"
    
    s3.Bucket(BUCKET_NAME).download_file(KEY, 'compfile2.gz')
    
    with gzip.open('compfile2.gz', 'rt') as f:
        file_content = f.read()
    print(file_content)
    
if __name__ == '__main__':
    main()



