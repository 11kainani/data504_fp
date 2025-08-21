import boto3
import pandas as pd
import json
import io


s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')


# Preview objects in the final project bucket:
bucket = s3_resource.Bucket('data-504-final-project')
count = 0
for obj in bucket.objects.all():
    print(obj.key)
    count += 1
    if count == 10:
        break


# Explore objects
# Academy: training scores
response = s3_client.get_object(Bucket='data-504-final-project', Key='Academy/Engineering_21_2019-07-15.csv')
content = response['Body'].read().decode('utf-8')
print(content)
    # Academy folder - scores, empty mean they were removed from training

# Talent: interview (json)
response = s3_client.get_object(Bucket='data-504-final-project', Key='Talent/13462.json')
content = response['Body'].read().decode('utf-8')
print(content)

# Talent: applicants contact details/month (those invited to attend a Sparta day)
response = s3_client.get_object(Bucket='data-504-final-project', Key='Talent/Dec2019Applicants.csv')
content = response['Body'].read(500).decode('utf-8')
print(content)

# Talent: Sparta day
response = s3_client.get_object(Bucket='data-504-final-project', Key='Talent/Sparta Day 2 January 2019.txt')
content = response['Body'].read().decode('utf-8')
print(content)



# Talent: combined data
#response = s3_client.get_object(Bucket='data-504-final-project', Key='Talent_Combined/combined_talent_decision_scores.csv')
#content = response['Body'].read().decode('utf-8')
#print(content)




