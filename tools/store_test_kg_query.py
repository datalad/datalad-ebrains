import requests
from sys import argv
import json
from os import environ
from uuid import UUID

from datalad_ebrains.kg_query import (
    get_auth_headers,
    get_token,
)


# use query endpoint for datasets
query_base = 'https://core.kg.ebrains.eu/v3-beta/queries'


def store_query(name, token, spec):
    url = "{}/{}".format(query_base, name)
    r = requests.put(
        url,
        data=spec,
        headers=get_auth_headers(token),
    )
    if r.ok:
        print('Successfully stored the query at %s ' % r.url)
    else:
        print('Problem with "put" protocol on url: %s ' % r.url )
        print(r)
        print(r.reason)
        print(r.content)


def query_dataset(name, token, dataset_id):
    url = "{}/{}/instances?stage=RELEASED&dataset_id={}".format(query_base, name, dataset_id)
    r = requests.get(
        url,
        headers=get_auth_headers(token),
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

    token = get_token('ebrains')

    query_name = argv[1]
    try:
        dataset_id = UUID(argv[2])
    except ValueError:
        dataset_id = None

    if dataset_id is None:
        store_query(query_name, token, open(argv[2], 'r'))
    else:
        results = query_dataset(query_name, token, dataset_id)
        if results:
            print(json.dumps(results))
