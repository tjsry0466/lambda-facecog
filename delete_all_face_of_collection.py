import boto3


def list_faces_in_collection(collection_id):

    maxResults = 99
    faces_count = 0
    tokens = True

    client = boto3.client('rekognition')
    response = client.list_faces(CollectionId=collection_id,
                                 MaxResults=maxResults)
    return response['Faces']


def delete_faces_from_collection(collection_id, faces):

    client = boto3.client('rekognition')

    response = client.delete_faces(CollectionId=collection_id,
                                   FaceIds=faces)

    print(str(len(response['DeletedFaces'])) + ' faces deleted:')
    for faceId in response['DeletedFaces']:
        print(faceId)
    return len(response['DeletedFaces'])


def main():

    collection_id = 'Collection'

    faces = list_faces_in_collection(collection_id)
    face_list = []
    print(len(faces))
    for face in faces:
        face_list.append(face['FaceId'])

    faces_count = delete_faces_from_collection(collection_id, face_list)
    print("deleted faces count: " + str(faces_count))


if __name__ == "__main__":
    main()
