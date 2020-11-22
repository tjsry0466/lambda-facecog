import boto3


def search_face_in_collection(face_id, collection_id):
    threshold = 90
    max_faces = 2
    client = boto3.client('rekognition')

    response = client.search_faces(CollectionId=collection_id,
                                   FaceId=face_id,
                                   FaceMatchThreshold=threshold,
                                   MaxFaces=max_faces)

    face_matches = response['FaceMatches']
    print('Matching faces')
    print(face_matches)
    for match in face_matches:
        print('FaceId:' + match['Face']['FaceId'])
        print('Similarity: ' + "{:.2f}".format(match['Similarity']) + "%")
        print
    return len(face_matches)


def main():

    face_id = '9f390c01-6aab-472a-b770-e1f19d72d904'
    collection_id = 'Collection'

    # faces = []
    # faces.append(face_id)

    faces_count = search_face_in_collection(face_id, collection_id)
    print("faces found: " + str(faces_count))


if __name__ == "__main__":
    main()
