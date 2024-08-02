import os
import requests
import teslapy
import datetime as dt
import martens as mt

from dotenv import load_dotenv

load_dotenv()

tesla_username = os.environ['TESLA_USERNAME']
amber_site_id = os.environ['AMBER_SITE_ID']
amber_api_key = os.environ['AMBER_API_KEY']

period_start_date = dt.date(2024, 1, 1)
period_end_date = dt.date(2024, 6, 30)
daylight_sav = dt.date(2024, 4, 7)
period_length = 4


def month(date):
    return date.strftime('%Y%m')


def day(date):
    return date.date()


def iso_date(datetime):
    aest_timezone = dt.timezone(dt.timedelta(hours=10))
    aest_time = datetime.replace(tzinfo=aest_timezone)
    return aest_time.isoformat()


def bill_total(feed_in,usage):
    return feed_in + usage

def price_record(start_date, end_date):
    url = 'https://api.amber.com.au/v1/sites/{amber_site_id}/prices'.format(amber_site_id=amber_site_id)
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer {amber_api_key}'.format(amber_api_key=amber_api_key)
    }
    params = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'resolution': '5'
    }
    return requests.get(url, headers=headers, params=params).json()


with teslapy.Tesla(tesla_username) as tesla:
    batteries = tesla.battery_list()
    battery = batteries[0]

    def history_records(date):
        start_date = dt.datetime.combine(date, dt.datetime.min.time())
        end_date = start_date.replace(hour=22 if date < daylight_sav else 23, minute=59, second=59)
        data = battery.get_calendar_history_data(start_date=iso_date(start_date), end_date=iso_date(end_date))
        return data['time_series']

    usage_data = mt.initialise((period_end_date - period_start_date).days + 1, 'day_id') \
        .mutate(lambda day_id: period_start_date + dt.timedelta(days=day_id), 'date') \
        .mutate(history_records) \
        .json_explode('history_records') \
        .drop(['day_id', 'date']) \
        .mutate(lambda timestamp: dt.datetime.fromisoformat(timestamp), 'date') \
        .drop(['timestamp', 'raw_timestamp']) \
        .fill_none('0.0') \
        .replace(float, excluded_names=['date']) \

price_data = mt.initialise((period_end_date - period_start_date).days + 1, 'day_id') \
    .filter(lambda day_id: day_id % period_length == 0) \
    .mutate(lambda day_id: period_start_date + dt.timedelta(days=day_id), 'start_date') \
    .mutate(lambda start_date: min(period_end_date, start_date + dt.timedelta(days=period_length)), 'end_date') \
    .mutate(price_record).json_explode('price_record') \
    .rename_and_select({'nemTime': 'time_str', 'channelType': 'type', 'perKwh': 'cost'}) \
    .mutate(lambda time_str: dt.datetime.fromisoformat(time_str) - dt.timedelta(minutes=5), 'date') \
    .replace(float, ['cost']) \
    .column_squish(grouping_cols=['date'], headings='type', values='cost', prefix='amber_') \
    .headings_camel_to_snake

merged = price_data.merge(usage_data, on=['date'], how='inner') \
    .mutate(month) \
    .fill_none(0.0)

base_scenario = merged \
    .mutate(lambda total_grid_energy_exported, amber_feed_in: total_grid_energy_exported * amber_feed_in / 100000, 'feed_in') \
    .mutate(lambda grid_energy_imported, amber_general: grid_energy_imported * amber_general / 100000, 'usage') \
    .group_by(grouping_cols=['month'], other_cols=['feed_in', 'usage']) \
    .replace(lambda x: round(sum(x), 2), ['feed_in', 'usage']) \
    .with_constant('base_scenario', 'scenario') \
    .mutate(bill_total) \
    .select(['month', 'scenario', 'usage', 'feed_in', 'bill_total'])

no_battery_or_solar = merged \
    .mutate(lambda total_home_usage, amber_general: total_home_usage * amber_general / 100000, 'usage') \
    .group_by(grouping_cols=['month'], other_cols=['usage']) \
    .replace(lambda x: round(sum(x), 2), ['usage']) \
    .with_constant(0.0, 'feed_in') \
    .with_constant('no_battery_or_solar', 'scenario') \
    .mutate(bill_total) \
    .select(['month', 'scenario', 'usage', 'feed_in', 'bill_total'])

solar_only = merged \
    .mutate(lambda total_home_usage, total_solar_generation: max(total_home_usage - total_solar_generation, 0.0),
            'solar_only_usage') \
    .mutate(lambda total_home_usage, total_solar_generation: max(total_solar_generation - total_home_usage, 0.0),
            'solar_only_feed_in') \
    .mutate(lambda solar_only_feed_in, amber_feed_in: solar_only_feed_in * amber_feed_in / 100000, 'feed_in') \
    .mutate(lambda solar_only_usage, amber_general: solar_only_usage * amber_general / 100000, 'usage') \
    .group_by(grouping_cols=['month'], other_cols=['feed_in', 'usage']) \
    .replace(lambda x: round(sum(x), 2), ['feed_in', 'usage']) \
    .with_constant('solar_only', 'scenario') \
    .mutate(bill_total) \
    .select(['month', 'scenario', 'usage', 'feed_in', 'bill_total'])

battery_only = merged \
    .mutate(lambda grid_energy_exported_from_battery, amber_feed_in: grid_energy_exported_from_battery * amber_feed_in / 100000,'feed_in') \
    .mutate(lambda grid_energy_imported, amber_general, battery_energy_imported_from_solar: (battery_energy_imported_from_solar + grid_energy_imported) * amber_general / 100000,'usage') \
    .group_by(grouping_cols=['month'], other_cols=['feed_in', 'usage']) \
    .replace(lambda x: round(sum(x), 2), ['feed_in', 'usage']) \
    .with_constant('battery_only', 'scenario') \
    .mutate(bill_total) \
    .select(['month', 'scenario', 'usage', 'feed_in', 'bill_total'])


all_scenarios = mt.stack([base_scenario, no_battery_or_solar, solar_only, battery_only]) \
    .group_by(grouping_cols=['scenario'], other_cols=['bill_total']) \
    .replace(lambda x: round(sum(x), 2), ['bill_total']) \
    .select(['scenario', 'bill_total'])

print(all_scenarios)