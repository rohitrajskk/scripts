#!/home/rohit/appdev/bin/python
import json
import xml.etree.ElementTree as ET
from parse import compile
import argparse
import httpx

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
    res = httpx.get(url, params=params, timeout=None)
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
    res = httpx.get(url, params=params, timeout=None)
    if res.status_code != 200:
        return []
    data = res.json()
    data = data["data"]
    doc_ids = []
    for item in data:
        try:
            id_xml = ET.fromstring(item[6])
            info_str = id_xml.attrib['onclick']
            parsed_info = str_parser.parse(info_str)
            doc = {
                "doctorId": parsed_info[0],
                "regdNoValue": parsed_info[1]
            }
            doc_ids.append(doc)
        except ET.ParseError:
            print(item)
    return doc_ids


def main():
    parser = argparse.ArgumentParser(description="Collect Indian registered doctor detail from Government register")
    parser.add_argument("-sy", "--start_year", default=1947, type=int, help="Start year")
    parser.add_argument("-ey", "--end_year", default=2021, type=int, help="End year")
    parser.add_argument("-p", "--no_of_process", default=5, type=int, help="No of Parallel process to run")
    args = parser.parse_args()
    doc_tasks = []
    for year in range(args.end_year, args.start_year, -1):
        print("Collecting doctor data for year {}...".format(year))
        doc_id = get_doc_id(year=year)
        file_name = 'data/doc_id_{}.json'.format(year)
        with open(file_name, 'w') as fp:
            json.dump(doc_id, fp, indent=4)
        print("Doctor ID retrieved successfully for year {}".format(year))


if __name__ == "__main__":
    main()
