import json
import boto3
import requests

def detect_labels(photo, bucket):
    labels_res = []
    client=boto3.client('rekognition')
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}},
        MaxLabels=10)
    print(photo) 
    for label in response['Labels']:
        print ("Label: " + label['Name'])
        labels_res.append(label['Name'])
    return labels_res

def lambda_handler(event, context):
    # TODO implement
    print(event)
    s3_info = event['Records'][0]['s3']
    bucket_name = s3_info['bucket']['name']
    key_name = s3_info['object']['key']
    metadata = boto3.client('s3').head_object(Bucket=bucket_name, Key=key_name)
    print(metadata)
    k = metadata['ResponseMetadata']['HTTPHeaders']['x-amz-meta-customlabel']
    print(k)
    c = k.split(',')
    print(c)
    bucket = "storephoto9"
    for record in event['Records']:
        image_name = record["s3"]["object"]["key"]
        eventtime = record["eventTime"]
        #print(response)
        #print(image_name,eventtime)
        labels_res=detect_labels(image_name, bucket)
        print(labels_res)
    
    labels_res.extend(c)
    print(labels_res)
    temp=[]
    host= ""
    index = 'gkk'
    type = 'photo'
    url = host + '/' + index + '/' + type
    headers = {"Content-Type": "application/json"}
    format = {'objectkey':image_name,'timstamp':eventtime,'bucket':bucket,'labels':labels_res}
    response = requests.post(url, data=json.dumps(format).encode("utf-8"), headers=headers,auth=('admin', 'Admin@123'))
    dat = json.loads(response.text)
    print(dat)
    print("3")
    return {
        'statusCode': 200,
        'body': json.dumps('Labels added to ES')
    }