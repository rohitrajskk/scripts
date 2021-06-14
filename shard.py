#!/home/rohit/appdev/bin/python
import json


def aggregate_doc_details_json():
    with open('data/aggregate/doc_detail_agg.json', 'r') as fp:
        doc_db = json.load(fp)
        n = len(doc_db)
        db1 = doc_db[0:int(n/4)]
        db2 = doc_db[int(n/4): int(n/2)]
        db3 = doc_db[int(n/2):int(3*n/4)]
        db4 = doc_db[int(3*n/4): n]

    with open('data/aggregate/doc_detail_1.json', 'w') as fp1:
        json.dump(db1, fp1, indent=4)
    with open('data/aggregate/doc_detail_2.json', 'w') as fp2:
        json.dump(db2, fp2, indent=4)
    with open('data/aggregate/doc_detail_3.json', 'w') as fp3:
        json.dump(db3, fp3, indent=4)
    with open('data/aggregate/doc_detail_4.json', 'w') as fp4:
        json.dump(db4, fp4, indent=4)


if __name__ == "__main__":
    aggregate_doc_details_json()
