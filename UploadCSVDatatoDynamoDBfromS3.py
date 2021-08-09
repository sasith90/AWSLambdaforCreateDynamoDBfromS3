import boto3
s3_client = boto3.client("s3")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('cent_info')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name)
    data = resp ['Body'].read().decode("utf-8")
    centers = data.split("\n")
    for cen in centers:
        print(cen)
        cen_data = cen.split(",")
        # Adding to DynamoDB #
        table.put_item(Item ={
            'cename' : cen_data[0],
            'ceno' : cen_data[1],
            'celetter' : cen_data[2],
            'tp1' : cen_data[3],
            'tp2' : cen_data[4],
            'mostrat' : cen_data[5],
            'moend' : cen_data[6],
            'tustart' : cen_data[7],
            'tuend' : cen_data[8],
            'westart' : cen_data[9],
            'weend' : cen_data[10],
            'thstart' : cen_data[11],
            'thend' : cen_data[12],
            'frstart' : cen_data[13],
            'frend' : cen_data[14],
            'ststart' : cen_data[15],
            'stend' : cen_data[16],
            'sunstart' : cen_data[17],
            'suend' : cen_data[18],
            'icename' : cen_data[19],
            'scename' : cen_data[20],
        })

    
    