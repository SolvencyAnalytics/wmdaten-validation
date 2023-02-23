import psycopg2
import pandas as pd
import os

LEGACY_DB_HOST = os.getenv("LEGACY_DB_HOST")
WMDATEN_COLUMNS = [
    'WKN', 'ISIN', 'GV646B_1', 'GV646B_2',
    'GV646B_3', 'GV646B_4', 'GV646C_1', 'GV646C_2', 'GV646C_3', 'GV646C_4',
    'section.GD609', 'section.idVal', 'section.GD205B',
    'section.GD609B', 'section.GD609D', 'section.GD609F',
    'section.table.idName', 'GV233A', 'GV233B', 'GV233C', 'GV233D',
    'GV233E', 'GV233F', 'GV646A_1', 'GV646A_2', 'GV646A_3', 'GV646A_4',
    'GV646D_1', 'GV646D_2', 'GV646D_3', 'GV646D_4', 'GV646E_1', 'GV646E_2',
    'GV646E_3', 'GV646E_4', 'section.table_GV233.idName',
    'section.table_GV646.idName',
    'section.GD240', 'section.GD245', 'section.GD622PW', 'section.table_GV222.idName',
    'section.table_GV233.rows',
    'section.table_GV222.rows'
]

def fetch_results_from_db() -> pd.DataFrame:
    connection = psycopg2.connect(LEGACY_DB_HOST)
    connection.set_session(autocommit=True)
    
    cursor = connection.cursor()
    cursor.execute(
        """
            with enrichments as (
                select enrichments.enrichment, requests.request_date::date as request_date
                from api_cache.enrichments enrichments
                join api_cache.responses responses
                on enrichments.response_id = responses.id
                join api_cache.requests requests
                on responses.request_id = requests.id
                join api_cache.clients clients
                on requests.client_id = clients.id
                join api_cache.providers providers
                on requests.provider_id = providers.id
                where
                providers.name = 'WMDaten'
            ), ids as (
            select
                enrichments.request_date,
            unique_ids.key as unique_id,
            unique_ids.value as data
            from
                enrichments,
                lateral jsonb_each(enrichments.enrichment) unique_ids
            )
            select unique_id, request_date, data from ids
            group by unique_id, request_date, data
            order by unique_id, request_date
            ;
        """
    )
    
    return pd.DataFrame(cursor.fetchall(), columns=['isin', 'request_date', 'result'])
    

def write_results(path: str, results: pd.DataFrame):
    results.to_csv(path, sep=',', index=False)


def read_results(path: str) -> pd.DataFrame:
    return pd.read_csv(path, sep=',', header=0, dtype=str)

import ast
    
def parse_duplicates(records: pd.DataFrame):
    duplicate_records = records[records[['isin', 'request_date']].duplicated(keep=False)].reset_index(drop=True)

    converted_json = pd.json_normalize(duplicate_records['result'].apply(ast.literal_eval))
    expanded_records = pd.concat([duplicate_records[['isin', 'request_date']], converted_json[list(converted_json.columns)]], axis=1)
    expanded_records = expanded_records.fillna(value="NotAvailable")

    duplicates_grouped = expanded_records.groupby(['isin', 'request_date'])    
    duplicate_groups = list(duplicates_grouped.groups.keys())
    
    number_of_groups = len(duplicate_groups)
    for idx, group in enumerate(duplicate_groups):
        groupetto = expanded_records[(expanded_records['isin'] == group[0]) & (expanded_records['request_date'] == group[1])]
        
        try: 
            if not groupetto.duplicated(subset=WMDATEN_COLUMNS, keep=False).any():
                print(f"Found difference in record for isin: {group[0]} on request_date: {group[1]}")
                groupetto.to_csv("groups_with_differences.csv", index=False, sep=',', mode='a')
            elif idx%100 == 0:
                print(f"Checked {idx} groups out of {number_of_groups}, no difference in records")
        except:
            groupetto.to_csv("groups_with_lists.csv", index=False, sep=',', mode='a')


def main():
    # results = fetch_results_from_db()
    # write_results('results_by_request_date.csv', results)
    
    results = read_results('results_by_request_date.csv')
    parse_duplicates(results)

if __name__ == '__main__':
  main()

# section.GD609B,section.GD609D == STR_WMD_LEI_VALID_TO,