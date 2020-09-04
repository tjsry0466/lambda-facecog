import json
import boto3
import urllib

import pymysql.cursors

print('Loading function')
client = boto3.client('rekognition')
connection = pymysql.connect(host='facecog-rds.cwct330tepas.ap-northeast-2.rds.amazonaws.com',
                             user='facecog',
                             password='!j14682533',
                             db='facecog',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


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
                                  ExternalImageId=photo.split('/')[1],
                                  MaxFaces=1,
                                  QualityFilter="AUTO",
                                  DetectionAttributes=['ALL'])

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        return False

    return response['FaceRecords']


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


def query(method, sql, params=[]):

    result = 0

    with connection.cursor() as cursor:
        result = cursor.execute(sql, params)
        if method == 'select':
            result = cursor.fetchall()
        elif method == 'insert':
            isCommit = 1
        cursor.close()

    return result


def search_face_in_collection(face_id, collection_id):
    threshold = 70
    max_faces = 2

    response = client.search_faces(CollectionId=collection_id,
                                   FaceId=face_id,
                                   FaceMatchThreshold=threshold,
                                   MaxFaces=max_faces)

    face_matches = response['FaceMatches']
    # print('Matching faces')
    # for match in face_matches:
    #     print('FaceId:' + match['Face']['FaceId'])
    #     print('Similarity: ' + "{:.2f}".format(match['Similarity']) + "%")
    #     print

    return face_matches


def lambda_handler(event, context):
    isCommit = 0
    connection = pymysql.connect(host='facecog-rds.cwct330tepas.ap-northeast-2.rds.amazonaws.com',
                                 user='facecog',
                                 password='!j14682533',
                                 db='facecog',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    bucket = event['Records'][0]['s3']['bucket']['name']
    photo = event['Records'][0]['s3']['object']['key']
    collection_id = 'Collection'
    print(bucket, photo)
    facedata = add_faces_to_collection(bucket, photo, collection_id)
    print('아마존에 index_faces요청')

    f = facedata[0]['Face']
    fd = facedata[0]['FaceDetail']
    lm = facedata[0]['FaceDetail']['Landmarks']
    faceId = f['FaceId']

    sql = 'SELECT count(*) as count FROM face WHERE faceId = %s'
    params = [faceId]
    result = query('select', sql, params)
    print('존재하는지 조회')

    # photoname = photo.split('/')[1]
    # sql = 'SELECT * FROM face WHERE id IN (SELECT face_id FROM user_face WHERE user_id = (SELECT user_id FROM user_face JOIN face ON face.id = user_face.face_id WHERE face.ExternalImageId= %s))'
    # params = [photoname]
    # haveface = query('select', sql, params)

    if result[0]['count'] == 0:
        print('이미지 미존재')
        # insert face
        sql = "INSERT INTO face(faceId, rec_top, rec_left, rec_width, rec_height, imageId, ExternalImageId, confidence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        params = (f['FaceId'], f['BoundingBox']['Top'], f['BoundingBox']['Left'], f['BoundingBox']['Width'],
                  f['BoundingBox']['Height'], f['ImageId'], f['ExternalImageId'], f['Confidence'])
        result = query('insert', sql, params)

        # select key
        sql = 'SELECT LAST_INSERT_ID() as face_id'
        result = query('select', sql)
        face_id = result[0]['face_id']

        # insert faceDetail
        sql = 'INSERT INTO faceDetail(face_id, ageRange_low, ageRange_high, Smile, smileConfidence, eyeglasses, eyeglassesConfidence, sunglasses, sunglassesConfidence, gender, genderConfidence, beard, beardConfidence, mustache, mustacheConfidence, eyesOpen, eyesOpenConfidence, mouthOpen, mouthopenConfidence, supprised, calm, confused, disgusted, fear, happy, angry, sad, poseRoll, poseYaw, posePitch, quialityBrightness, qualitySharpness, confidence) VALUES (' + ('%s,'*32) + '%s)'
        params = [face_id, fd['AgeRange']['Low'], fd['AgeRange']['High'], fd['Smile']['Value'], fd['Smile']['Confidence'], fd['Eyeglasses']['Value'], fd['Eyeglasses']['Confidence'], fd['Sunglasses']['Value'], fd['Sunglasses']['Confidence'], fd['Gender']['Value'], fd['Gender']['Confidence'], fd['Beard']['Value'], fd['Beard']['Confidence'], fd['Mustache']['Value'], fd['Mustache']['Confidence'], fd['EyesOpen']['Value'], fd['EyesOpen']
                  ['Confidence'], fd['MouthOpen']['Value'], fd['MouthOpen']['Confidence'], fd['Emotions'][0]['Confidence'], fd['Emotions'][1]['Confidence'], fd['Emotions'][2]['Confidence'], fd['Emotions'][3]['Confidence'], fd['Emotions'][4]['Confidence'], fd['Emotions'][5]['Confidence'], fd['Emotions'][6]['Confidence'], fd['Emotions'][7]['Confidence'], fd['Pose']['Roll'], fd['Pose']['Yaw'], fd['Pose']['Pitch'], fd['Quality']['Brightness'], fd['Quality']['Sharpness'], fd['Confidence']]
        result = query('insert', sql, params)

        # insert faceLandMarks
        sql = 'INSERT INTO faceLandMarks(face_id, eyeLeftX, eyeLeftY, eyeRightX, eyerightY, mouthLeftX, mouthLeftY, mouthRightX, mouthRightY, noseX, noseY, leftEyeBrowLeftX, leftEyeBrowLeftY, leftEyeBrowRightX, leftEyeBrowRightY, rightEyeBrowLeftX, rightEyeBrowLeftY, rightEyeBrowRightX, rightEyeBrowRightY, rightEyeBrowUpX, rightEyeBrowUpY, leftEyeLeftX, leftEyeLeftY, leftEyeRightX, leftEyeRightY, leftEyeUpX, leftEyeUpY, leftEyeDownX, leftEyeDownY, rightEyeLeftX, rightEyeLeftY, rightEyeRightX, rightEyeRightY, rightEyeUpX, rightEyeUpY, rightEyeDownX, rightEyeDownY, noseLeftX, noseLeftY, noseRightX, noseRightY, mouthUpX, mouthUpY, mouthDownX, mouthDownY, leftPupilX, leftPupilY, rightPupilX, rightPupilY, upperJawlineLeftX, upperJawlineLeftY, midJawlineLeftX, midJawlineLeftY, chinBottomX, chinBottomY, midJawlineRightX, midJawlineRightY, upperJawlineRightX, upperJawlineRightY) VALUES (' + ('%s,'*(59-1)) + '%s)'
        params = [face_id, lm[0]['X'], lm[0]['Y'], lm[1]['X'], lm[1]['Y'], lm[2]['X'], lm[2]['Y'], lm[3]['X'], lm[3]['Y'], lm[4]['X'], lm[4]['Y'], lm[5]['X'], lm[5]['Y'], lm[6]['X'], lm[6]['Y'], lm[7]['X'], lm[7]['Y'], lm[8]['X'], lm[8]['Y'], lm[9]['X'], lm[9]['Y'], lm[10]['X'], lm[10]['Y'], lm[11]['X'], lm[11]['Y'], lm[12]['X'], lm[12]['Y'], lm[13]['X'], lm[13]['Y'], lm[14]['X'], lm[14]['Y'],
                  lm[15]['X'], lm[15]['Y'], lm[16]['X'], lm[16]['Y'], lm[17]['X'], lm[17]['Y'], lm[18]['X'], lm[18]['Y'], lm[19]['X'], lm[19]['Y'], lm[20]['X'], lm[20]['Y'], lm[21]['X'], lm[21]['Y'], lm[22]['X'], lm[22]['Y'], lm[23]['X'], lm[23]['Y'], lm[24]['X'], lm[24]['Y'], lm[25]['X'], lm[25]['Y'], lm[26]['X'], lm[26]['Y'], lm[27]['X'], lm[27]['Y'], lm[28]['X'], lm[28]['Y']]
        result = query('insert', sql, params)

    print('이미지가 추가된 상태임')
    print('같은 얼굴이 존재하는지 검색하는 로직')
    same_face = []
    faces = search_face_in_collection(faceId, collection_id)

    print('같은얼굴 존재 갯수:' + str(len(faces)))
    if len(faces):
        same_face = faces[0]['Face']['FaceId']
        # 얼굴 그룹 존재 여부
        sql = 'SELECT faceGroup_id FROM face_faceGroup WHERE face_id = (SELECT id FROM face WHERE faceId = %s)'
        params = [same_face]
        result = query('select', sql, params)

        if len(result) == 0:
            print('얼굴 그룹이 존재 하지않음. 데이터 추가작업시작')
            sql = 'INSERT INTO faceGroup() VALUES ()'
            result = query('insert', sql)
            print(result)

            # 방금 추가한 얼굴 그룹 id 얻기
            # select key
            sql = 'SELECT LAST_INSERT_ID() as faceGroup_id'
            result = query('select', sql)
            faceGroup_id = result[0]['faceGroup_id']

            # 얼굴과 얼굴 그룹 연결
            sql = 'INSERT INTO face_faceGroup(face_id, faceGroup_id) VALUES ((SELECT id FROM face WHERE faceId = %s), %s)'
            params = [same_face, faceGroup_id]
            result = query('insert', sql, params)
            print(result)
        else:
            print('얼굴 그룹 존재함!!')
            faceGroup_id = result[0]['faceGroup_id']

        # 얼굴과 얼굴 그룹 연결
        sql = 'INSERT INTO face_faceGroup(face_id, faceGroup_id) VALUES ((SELECT id FROM face WHERE faceId = %s), %s)'
        params = [faceId, faceGroup_id]
        result = query('insert', sql, params)
        print(result)

    if isCommit:
        connection.commit()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
        # 'content': json.dumps(bucket)
    }


        # 얼굴 그룹에 데이터 추가
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