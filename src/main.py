import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd

DEBUG = True
FORCE_UPDATE = False
SOURCE_URL = 'https://www.coronavirus2020.kz/'
SOURCE_CSS_PATH = 'body > div.mainContainer > div.wrap_cov_cont > div.table_info_cont.tabl_vactination ' \
    + '> table > tbody > tr > td.norm_font'
TARGET_PATHS = {
    'snapshot': '../data/snapshot.csv',
    'first_cumulative': '../data/first_cumulative.csv',
    'first_daily': '../data/first_daily.csv',
    'second_cumulative': '../data/second_cumulative.csv',
    'second_daily': '../data/second_daily.csv',
}
REGIONS = {
    'г. Нур-Султан': 'nursultan',
    'г. Алматы': 'almaty',
    'г. Шымкент': 'shyment',
    'Акмолинская область': 'akmola',
    'Актюбинская область': 'aktobe',
    'Алматинская область': 'almaty_region',
    'Атырауская область': 'atyrau',
    'Восточно-Казахстанская область': 'east_kazakhstan',
    'Жамбылская область': 'jambyl',
    'Западно-Казахстанская область': 'west_kazakhstan',
    'Карагандинская область': 'karagandy',
    'Костанайская область': 'kostanay',
    'Кызылординская область': 'kyzylorda',
    'Мангистауская область': 'mangistau',
    'Павлодарская область': 'pavlodar',
    'Северо-Казахстанская область': 'north_kazakhstan',
    'Туркестанская область': 'turkestan',
}
HEADERS = ['region_en']

print('Requesting data from "{}"'.format(SOURCE_URL))
result = requests.get(SOURCE_URL, verify=False)
soup = BeautifulSoup(result.content, features='html.parser')
results = soup.select(SOURCE_CSS_PATH)

if len(results) != len(REGIONS):
    raise Exception('Invalid content')

target_data = {}
for item in results:
    item = item.parent
    region_en = None
    region_ru = None
    first_cumulative = None
    first_daily = None
    second_cumulative = None
    second_daily = None

    cells = item.select('td')
    if len(cells) != 3:
        raise Exception('Unexpected content! {}'.format(item))

    if not region_en:
        region_ru = cells[0].string
        if region_ru not in REGIONS:
            raise Exception('Unexpected region "{}"! ITEM: {}'.format(region_ru, item))
        region_en = REGIONS[region_ru]

        first_cumulative = cells[1].string
        if not first_cumulative.isnumeric():
            raise Exception('Unexpected cumulative format "{}"! ITEM: {}'.format(first_cumulative, item))

        second_cumulative = cells[2].string
        if not second_cumulative.isnumeric():
            raise Exception('Unexpected cumulative format "{}"! ITEM: {}'.format(second_cumulative, item))

    target_data[region_en] = {
        'region_en': region_en,
        'region_ru': region_ru,
        'first_cumulative': first_cumulative,
        'first_daily': first_daily,
        'second_cumulative': second_cumulative,
        'second_daily': second_daily,
    }

for target_type in TARGET_PATHS:
    target_path = TARGET_PATHS[target_type]

    df = pd.read_csv(target_path)

    if target_type == 'snapshot':

        print('Processing "{}" - to check for updates!'.format(target_type))

        has_update = FORCE_UPDATE

        target_columns = ['first', 'second']

        for target_column in target_columns:
            for i, row in df.iterrows():
                region_en = row['region_en']
                item = target_data[region_en]

                if target_column == 'first':
                    today_value = item['first_cumulative']
                else:
                    today_value = item['second_cumulative']

                existing_value = row[target_column]

                df.loc[i, target_column] = today_value
                new_value = int(today_value) - int(existing_value)

                if DEBUG:
                    print('{} ({}): today={}, existing={}, new={}'.format(
                        region_en, target_column, today_value, existing_value, new_value
                    ))

                if new_value is None:
                    raise Exception('Unexpected value in "{}": "{}"'.format(region_en, new_value))

                if target_column == 'first':
                    target_data[region_en]['first_daily'] = new_value
                else:
                    target_data[region_en]['second_daily'] = new_value

                if int(existing_value) != int(today_value):
                    has_update = True

        if DEBUG:
            print(target_data)

        if has_update:
            print('Updates found!')
        else:
            print('No updates found!')
            continue

    else:

        if has_update:
            if FORCE_UPDATE:
                print('Processing "{}" - forced update!'.format(target_type))
            else:
                print('Processing "{}" - updates found!'.format(target_type))
        else:
            continue

        target_column = datetime.datetime.today().strftime('%Y-%m-%d')

        df[target_column] = 0
        for i, row in df.iterrows():
            region_en = row['region_en']
            item = target_data[region_en]
            today_value = item[target_type]
            df.loc[i, target_column] = today_value

    df.to_csv(target_path, index=False)

print('Process complete!')
