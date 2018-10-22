import csv
import datetime
import json
import os
import tempfile
from collections import defaultdict

import boto3 as boto3
from botocore.vendored import requests

s3_client = boto3.client('s3')

# https://www.getthedata.com/open-postcode-geo
postcode = 0,
status = 1
usertype = 2
easting = 3
northing = 4
positional_quality_indicator = 5
country = 6
latitude = 7
longitude = 8
postcode_no_space = 9
postcode_fixed_width_seven = 10
postcode_fixed_width_eight = 11
postcode_area = 12
postcode_district = 13
postcode_sector = 14
outcode = 15
incode = 15

def get_env_or_fail(key):
    value = os.getenv(key)
    if value is None:
        raise Exception("Setting '{}' Missing".format(key))

    return value

S3_BUCKET = get_env_or_fail('S3_BUCKET')
S3_KEY_PREFIX = get_env_or_fail('S3_KEY_PREFIX')

DATA_RANGE_YEAR_START = 2015
DATA_RANGE_MONTH_START = 9

_DAYS_IN_MONTH = [-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def get_crime_data(lat, lng, date):

    print('Importing for lat:{} lng:{} date:{}'.format(lat, lng, date))

    # https://data.police.uk/docs/method/crime-street/
    url = "https://data.police.uk/api/crimes-street/all-crime?" \
          "lat={}&" \
          "lng={}&" \
          "date={}".format(lat, lng, date)

    print(url)

    r = requests.get(url)
    # https://data.police.uk/api/crimes-street/all-crime?lat=52.629729&lng=-1.131592&date=2017-01
    print("Status Code: " + str(r.status_code))

    crimes = json.loads(r.content)

    if crimes:
        crime_types = defaultdict(lambda: 0)

        for crime in crimes:
            crime_types[crime['category']] += 1

        return crime_types

def import_data():
    now = datetime.datetime.now()

    print('Starting Import')

    postcode_file = s3_client.get_object(Bucket=S3_BUCKET, Key='postcodes/data.csv')

    print('Loaded Postcodes')

    for year in range(DATA_RANGE_YEAR_START, now.year):
        for month in range(1, 13):
            if year == DATA_RANGE_YEAR_START:
                if month < DATA_RANGE_MONTH_START:
                    continue

            s3_prefix = '{}/year={}/month={}'.format(S3_KEY_PREFIX,
                                                     year,
                                                     datetime.date(year, month, 1).strftime('%B')
                                                     )
            list_response = s3_client.list_objects_v2(Bucket=S3_BUCKET,
                                                      Prefix=s3_prefix + '/data.csv')

            if list_response.get('KeyCount') == 0:

                date = datetime.date(year, month, 1).strftime('%Y-%m')

                crime_categories = get_crime_categories(date)

                with tempfile.NamedTemporaryFile(mode='w+t') as temp:
                    with open(temp.name, 'w') as fake_csv:

                        for postcode_row in get_postcode_list(postcode_file):

                            crime_data = get_crime_data(postcode_row[latitude],
                                                       postcode_row[longitude],
                                                       date
                                                       )

                            if crime_data:
                                crime_data['postcode'] = postcode_row[postcode[0]]
                                writer = csv.DictWriter(fake_csv, fieldnames=['postcode'] + crime_categories)
                                writer.writerow(crime_data)

            temp.seek(0)

            s3_client.put_object(Body=temp.read(), Bucket=S3_BUCKET,
                              Key='{}/data.csv'.format(s3_prefix))

    print('Finished Import')


def get_postcode_list(postcode_file):
    reader = csv.reader(postcode_file['Body'].read(), delimiter=',')

    postcodes = (row for row in reader)

    for postcode_row in postcodes:
        yield postcode_row


def get_crime_categories(date):
    url = 'https://data.police.uk/api/crime-categories?date={}'.format(date)
    r = requests.get(url)
    categories = json.loads(r.content)
    category_names = []
    for category in categories:
        category_names.append(category['url'])

    return category_names

if __name__ == '__main__':
    import_data()