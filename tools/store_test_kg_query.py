import requests
from sys import argv
import json
from os import environ
from uuid import UUID

# get this from https://nexus-iam.humanbrainproject.org/v0/oauth2/authorize
if 'EBRAINS_TOKEN' not in environ:
    raise RuntimeError(
        'Provide access token via EBRAINS_TOKEN environment variable')

authheaders = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer {}'.format(environ['EBRAINS_TOKEN'])
}

# use query endpoint for datasets
query_base = "https://kg.humanbrainproject.eu/query/minds/core/dataset/v1.0.0"


def store_query(name, spec):
    url = "{}/{}".format(query_base, name)
    r = requests.put(
        url,
        data=spec,
        headers=authheaders,
    )
    if r.ok:
        print('Successfully stored the query at %s ' % r.url)
    else:
        print('Problem with "put" protocol on url: %s ' % r.url )
        print(r)


def query_dataset(name, dataset_id):
    url = "{}/{}/instances?databaseScope=RELEASED&datasetId={}".format(
        query_base,
        name,
        dataset_id,
    )
    r = requests.get(
        url,
        headers=authheaders,
    )
    if not r.ok:
        print('Problem with "get" protocol on url: %s ' % r.url )
        print(r.status_code)
        print(r.text)
        return

    results = json.loads(r.content)
    return results


if __name__ == "__main__":
    if len(argv) != 3:
        print(
            "USAGE: {} <query_name> <query_spec.json>|<dataset_id>".format(
                argv[0]))
        exit(1)

    query_name = argv[1]
    try:
        dataset_id = UUID(argv[2])
    except ValueError:
        dataset_id = None

    if dataset_id is None:
        store_query(query_name, open(argv[2], 'r'))
    else:
        results = query_dataset(query_name, dataset_id)
        if results:
            print(json.dumps(results))
