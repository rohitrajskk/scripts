#!/home/rohit/appdev/bin/python
import json
import os
import fnmatch


def aggregate_doc_details_json():
    json_file_list = []
    for root, dirnames, filenames in os.walk("./data/"):
        for filename in fnmatch.filter(filenames, "*_detail*.json"):
            filepath = os.path.join(root, filename)
            json_file_list.append(filepath)

    doc_details = []
    for f in json_file_list:
        with open(f, 'r') as fp:
            doc_details.extend(json.load(fp))
    with open('data/doc_detail_agg.json', 'w') as fp:
        json.dump(doc_details, fp, indent=4)


if __name__ == "__main__":
    aggregate_doc_details_json()
