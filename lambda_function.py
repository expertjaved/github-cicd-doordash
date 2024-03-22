import boto3
import pandas as pd
import json

# Initialize S3 and SNS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Define the SNS topic ARN
sns_topic_arn = 'arn:aws:sns:ap-south-1:399908068704:doordashtopic'

def lambda_handler(event, context):
    try:
        print(event)
        # Get the S3 bucket and object key from the lambda event trigger
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print('Bucket ->', bucket)
        print('Key ->', key)

        # Use boto3 to get the JSON file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response["Body"].read().decode("utf-8")

        # Read the content into a pandas DataFrame
        data = pd.read_json(file_content)

        # Filter records where status is "delivered"
        filtered_data = data[data['status'] == 'delivered']

        # Convert the filtered DataFrame back to JSON string
        filtered_json = filtered_data.to_json(orient='records')

        # Upload the filtered JSON to the target S3 bucket
        target_bucket = 'doordash-target-zn2'
        target_key = f'{key.split("/")[-1].split(".")[0]}_filtered.json'
        s3_client.put_object(Body=filtered_json, Bucket=target_bucket, Key=target_key)

        # Publish success message to SNS topic
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message='Filtered JSON file successfully uploaded to S3.'
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('Success')
        }

    except Exception as e:
        # Publish failure message to SNS topic
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=f'Error processing file: {str(e)}'
        )
        return {
            'statusCode': 500,
            'body': json.dumps('Error')
        }
