import json
import boto3
import urllib

print('Loading function')


def detect_faces(photo, bucket):

    response = client.detect_faces(
        Image={'S3Object': {'Bucket': bucket, 'Name': photo}}, Attributes=['ALL'])

    print('Detected faces for ' + photo)
    # print(response)
    # for faceDetail in response['FaceDetails']:
    # print('The detected face is between ' + str(faceDetail['AgeRange']['Low'])
    #       + ' and ' + str(faceDetail['AgeRange']['High']) + ' years old')
    # print('Here are the other attributes:')
    # print(json.dumps(faceDetail, indent=4, sort_keys=True))
    return len(response['FaceDetails'])


def create_collection(collection_id):

    # Create a collection
    print('Creating collection:' + collection_id)
    response = client.create_collection(CollectionId=collection_id)
    print('Collection ARN: ' + response['CollectionArn'])
    print('Status code: ' + str(response['StatusCode']))
    print('Done...')


def list_collections():

    max_results = 2

    # Display all the collections
    print('Displaying collections...')
    response = client.list_collections(MaxResults=max_results)
    collection_count = 0
    done = False

    while done == False:
        collections = response['CollectionIds']

        for index, collection in enumerate(collections):
            print(index, collection)
            collection_count += 1
        if 'NextToken' in response:
            nextToken = response['NextToken']
            response = client.list_collections(
                NextToken=nextToken, MaxResults=max_results)

        else:
            done = True

    return collection_count


def describe_collection(collection_id):

    print('Attempting to describe collection ' + collection_id)

    try:
        response = client.describe_collection(CollectionId=collection_id)
        print("Collection Arn: " + response['CollectionARN'])
        print("Face Count: " + str(response['FaceCount']))
        print("Face Model Version: " + response['FaceModelVersion'])
        print("Timestamp: " + str(response['CreationTimestamp']))

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('The collection ' + collection_id + ' was not found ')
        else:
            print('Error other than Not Found occurred: ' +
                  e.response['Error']['Message'])
    print('Done...')


def delete_collection(collection_id):
    print('Attempting to delete collection ' + collection_id)
    status_code = 0
    try:
        response = client.delete_collection(CollectionId=collection_id)
        status_code = response['StatusCode']

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('The collection ' + collection_id + ' was not found ')
        else:
            print('Error other than Not Found occurred: ' +
                  e.response['Error']['Message'])
        status_code = e.response['ResponseMetadata']['HTTPStatusCode']
    return(status_code)


def add_faces_to_collection(bucket, photo, collection_id):

    response = client.index_faces(CollectionId=collection_id,
                                  Image={'S3Object': {
                                      'Bucket': bucket, 'Name': photo}},
                                  ExternalImageId=photo,
                                  MaxFaces=1,
                                  QualityFilter="AUTO",
                                  DetectionAttributes=['ALL'])

    print('Results for ' + photo)
    print('Faces indexed:')
    for faceRecord in response['FaceRecords']:
        print('  Face ID: ' + faceRecord['Face']['FaceId'])
        print('  Location: {}'.format(faceRecord['Face']['BoundingBox']))

    print('Faces not indexed:')
    for unindexedFace in response['UnindexedFaces']:
        print(' Location: {}'.format(
            unindexedFace['FaceDetail']['BoundingBox']))
        print(' Reasons:')
        for reason in unindexedFace['Reasons']:
            print('   ' + reason)
    return len(response['FaceRecords'])


def list_faces_in_collection(collection_id):

    maxResults = 2
    faces_count = 0
    tokens = True

    response = client.list_faces(CollectionId=collection_id,
                                 MaxResults=maxResults)

    print('Faces in collection ' + collection_id)

    while tokens:

        faces = response['Faces']

        for face in faces:
            print(face)
            faces_count += 1
        if 'NextToken' in response:
            nextToken = response['NextToken']
            response = client.list_faces(CollectionId=collection_id,
                                         NextToken=nextToken, MaxResults=maxResults)
        else:
            tokens = False
    return faces_count


def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    photo = event['Records'][0]['s3']['object']['key']
    collection_id = 'Collection'

    indexed_faces_count = add_faces_to_collection(bucket, photo, collection_id)
    print("Faces indexed count: " + str(indexed_faces_count))

    # faces_count=list_faces_in_collection(collection_id)
    # print("faces count: " + str(faces_count))
    # collection_id='family_collection'
    # status_code=delete_collection(collection_id)
    # print('Status code: ' + str(status_code))

    # bucket='bucket'
    # collectionId='MyCollection'
    # fileName='input.jpg'
    # threshold = 70
    # maxFaces=2

    # response=client.search_faces_by_image(CollectionId=collectionId,
    #                             Image={'S3Object':{'Bucket':bucket,'Name':fileName}},
    #                             FaceMatchThreshold=threshold,
    #                             MaxFaces=maxFaces)

    # faceMatches=response['FaceMatches']
    # print ('Matching faces')
    # for match in faceMatches:
    #         print ('FaceId:' + match['Face']['FaceId'])
    #         print ('Similarity: ' + "{:.2f}".format(match['Similarity']) + "%")
    #         print

    # 얼굴 감지
    # face_count=detect_faces(imageName, bucket)
    # print("Faces detected: " + str(face_count))

    # 컬렉션 생성
    # collection_id='Collection'
    # create_collection(collection_id)

    # 컬렉션 리스트
    # collection_count=list_collections()
    # print("collections: " + str(collection_count))

    # 컬렉션 설명
    # collection_id='Collection'
    # describe_collection(collection_id)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
        # 'content': json.dumps(bucket)
    }
