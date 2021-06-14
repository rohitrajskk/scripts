#!/home/rohit/appdev/bin/python
import asyncio
import json
import httpx
import xml.etree.ElementTree as ET
from parse import compile
from multiprocessing import Pool
import argparse

url = 'https://www.nmc.org.in/MCIRest/open/getPaginatedData'
str_parser = compile("openDoctorDetailsnew('{}', '{}')")
detail_url = "https://www.nmc.org.in/MCIRest/open/getDataFromService?service=getDoctorDetailsByIdImr"


def get_no_of_doctor(year=2020):
    params = {
        "service": "getPaginatedDoctor",
        "start": 0,
        "length": 1,
        "year": year
    }
    res = httpx.get(url, params=params)
    if res.status_code == 200:
        data = res.json()
        return data["recordsTotal"]
    return []


def get_doc_id(year=2020):
    params = {
        "service": "getPaginatedDoctor",
        "start": 0,
        "length": get_no_of_doctor(year),
        "year": year
    }
    res = httpx.get(url, params=params)
    if res.status_code != 200:
        return []
    data = res.json()
    data = data["data"]
    doc_ids = []
    for item in data:
        id_xml = ET.fromstring(item[6])
        info_str = id_xml.attrib['onclick']
        parsed_info = str_parser.parse(info_str)
        doc = {
            "doctorId": parsed_info[0],
            "regdNoValue": parsed_info[1]
        }
        doc_ids.append(doc)
    return doc_ids


async def get_doc_detail(doc_id_chunk):
    tasks = []
    headers = {
        'Content-Type': 'application/json'
    }
    # timeout = httpx.Timeout(20.0, connect=120.0)
    client = httpx.AsyncClient(timeout=None)
    for _id in doc_id_chunk:
        tasks.append(client.post(detail_url, headers=headers, data=json.dumps(_id)))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    doctors = [result.json() for result in results if result.status_code == 200]
    await client.aclose()
    return doctors


async def get_doc_detail_year(year):
    print("Collecting doctor data for year {}...".format(year))
    with open('data/doc_id_{}.json'.format(year), 'r') as fp:
        doc_id = json.load(fp)
    doctor_detail = await get_doc_detail(doc_id)
    print("Doctor details retrieved successfully for year {}".format(year))
    file_name = 'data/doc_detail_{}.json'.format(year)
    with open(file_name, 'w') as fp:
        json.dump(doctor_detail, fp, indent=4)


def get_doc_details_shard(doc_ids, no_of_worker=10):
    doc_id_shards = []
    idx = 0
    len_per_shard = int(len(doc_ids) / no_of_worker)
    print(len_per_shard)
    for i in range(no_of_worker):
        doc_id_shards.append(doc_ids[idx:(idx + len_per_shard)])
        idx = idx + len_per_shard
    with Pool(no_of_worker) as p:
        doctor_details = p.map_async(get_doc_detail, doc_id_shards)
        data = doctor_details.get()
        return data


async def main():
    parser = argparse.ArgumentParser(description="Collect Indian registered doctor detail from Government register")
    parser.add_argument("-sy", "--start_year", default=1947, type=int, help="Start year")
    parser.add_argument("-ey", "--end_year", default=2021, type=int, help="End year")
    parser.add_argument("-p", "--no_of_process", default=5, type=int, help="No of Parallel process to run")
    args = parser.parse_args()
    doc_tasks = []
    for year in range(args.end_year, args.start_year, -1):
        doc_tasks.append(get_doc_detail_year(year))
    await asyncio.gather(*doc_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
