import datetime
import os
import tempfile
import zipfile
from io import BytesIO
from urllib.request import urlopen

import boto3 as boto3

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

DATA_RANGE_YEAR_START = 2013
DATA_RANGE_MONTH_START = 12

_DAYS_IN_MONTH = [-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def get_crime_data_archive(year, month):
    date = datetime.date(year, month, 1).strftime('%Y-%m')
    print('Importing crime archive for date:{}'.format(date))

    # https://data.police.uk/docs/method/crime-street/
    url = "https://data.police.uk/data/archive/{}.zip".format(date)

    print(url)

    resp = urlopen(url)

    existing_dates = []

    with zipfile.ZipFile(BytesIO(resp.read())) as my_zip_file:
        last_date_prefix = None
        temp_file = None
        for contained_file in my_zip_file.namelist():
            if 'street.csv' in contained_file:

                print("Processing File", contained_file)

                current_date_prefix = contained_file.split('/')[0]

                if current_date_prefix in existing_dates:
                    print("Already loaded, Skipping", current_date_prefix)
                else:
                    date_prefix_parts = current_date_prefix.split('-')

                    s3_prefix = '{}/year={}/month={}'.format(S3_KEY_PREFIX,
                                                             date_prefix_parts[0],
                                                             date_prefix_parts[1]
                                                             )
                    list_response = s3_client.list_objects_v2(Bucket=S3_BUCKET,
                                                              Prefix=s3_prefix + '/data.csv')

                    if list_response.get('KeyCount') == 0:
                        if not last_date_prefix:
                            last_date_prefix = current_date_prefix
                            temp_file = tempfile.TemporaryFile(mode='w+t')

                        if last_date_prefix != current_date_prefix:
                            print('Uploading file for period', current_date_prefix)
                            # Upload file
                            temp_file.seek(0)
                            current_prefix_parts = last_date_prefix.split('-')
                            s3_prefix = '{}/year={}/month={}'.format(S3_KEY_PREFIX,
                                                                     current_prefix_parts[0],
                                                                     current_prefix_parts[1]
                                                                     )
                            s3_client.put_object(Body=temp_file.read(), Bucket=S3_BUCKET,
                                                 Key='{}/data.csv'.format(s3_prefix))
                            existing_dates.append(last_date_prefix)
                            last_date_prefix = current_date_prefix

                            # Create new tempfile
                            temp_file = tempfile.TemporaryFile(mode='w+t')

                        for line in my_zip_file.open(contained_file).readlines()[1:]:
                            temp_file.write(str(line, 'UTF-8'))


def import_data():
    now = datetime.datetime.now()

    print('Starting Import')

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
                get_crime_data_archive(year, month)

    print('Finished Import')


if __name__ == '__main__':
    import_data()