import boto3


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
    faces = ['02b8852e-4676-4b8e-b914-9ead8b4dcc0b']

    faces_count = delete_faces_from_collection(collection_id, faces)
    print("deleted faces count: " + str(faces_count))


if __name__ == "__main__":
    main()
