import data_maker as d
from visual_search import BankSearchApi, CardSearchApi, EfincSearchApi
import pandas as pd
import datetime


m_input = {'banks': ['transType', 'transMemo', 'transClass'],
           'cards': ['merchantRegno', 'merchantName'],
           'efinances': ['merchantRegno', 'merchantName', 'transMemo', 'transTitle', 'payMethod']
           }

# src file names
file_names = {'banks': 'test_all_bank.txt',
              'cards': 'test_all_card.txt',
              'efinances': 'test_all_efinc.txt'}

# input variables
body_name_input = 'cards'
person_count = 0
event_count = 0
skip_persons = 101
person_max = 2
event_max = 300
dtype_mapping = {'transDtime': str, 'payMethod' : str}

# print out in terminal
printout_result = {
    'cs_company':['company.name', 'company.address.full'],
    'cs_only_companyName': ['company.name'],
    'cs_fran':['fran.name', 'branch', 'hq'],
    'cs_cate':['category.large', 'category.medium'],
    'label':[],
    'person':['freq', 'sum', 'label.id', 'label.labelClass'],
}

def get_elapsed_time():
    return datetime.datetime.now() - begin_time

# <decorated fn>
def elapsed_time(fn):
    def wrapper(*args, **kargs):
        print(f"{get_elapsed_time()} : {len(person_data.get(body_name))} from [{person_data.get('person')}]")
        a = get_elapsed_time()
        result = fn(*args,**kargs)
        b = get_elapsed_time()
        print(f"{b} : get result in {b-a}")
        return result
    return wrapper
    
@elapsed_time
def get_result(api_selected = None, body_name = None, data = None):
    return api_selected.get(body_name).request_api(data)

def format_2str(value):
    return f"{value:02d}"
def format_str(value):
    return f"{value}"

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    begin_time = datetime.datetime.now()
    d_num = begin_time.strftime('%m%d_%H%M')

    lab_result = pd.DataFrame()
    cs_result = pd.DataFrame()
    ps_result = pd.DataFrame()

    bank = BankSearchApi()
    card = CardSearchApi()
    efin = EfincSearchApi()

    # assign request input params
    api_select = {'banks': bank,
                  'cards': card,
                  'efinances': efin}
    body_name = body_name_input
    fp = f'./data/{file_names.get(body_name)}'
    
    for person_data in d.get_file(fp, api_name=body_name,
                                  person_max=person_max, event_max=event_max,
                                  skip_persons=skip_persons, dtype_mapping=dtype_mapping):
        # can skip rows
        # can skip person ids

        ## original result get
        # print(f"{get_elapsed_time()} : {len(person_data.get(body_name))} from [{person_data.get('person')}]")
        # a = get_elapsed_time()
        # result = api_select.get(body_name).request_api(person_data)
        # b = get_elapsed_time()
        
        
        
        # decoreated result fn call
        result = get_result(api_selected=api_select, body_name=body_name, data=person_data)
        
        person_count += 1
        event_count += event_max

        if not result.get('data'):
            print(f'NO RESULTS FOUND!')
            print(result)
            print('-----------------')
            print(person_data.get(body_name))
            break

        ldf = pd.json_normalize(result['data']['transaction']['labeler']['results'], record_path='labels',
                                             meta='tranId')
        ldf = ldf.sort_values(by='tranId')
        ldf['eid'] = pd.factorize(ldf['tranId'])[0]
        # print(f"{b} : found {len(ldf)} labels in {b-a}")
        ldf['api_name'] = body_name
        ldf['personId'] = person_data['person']['id']

        cdf = pd.json_normalize(result['data']['transaction']['companySearch']['results'], max_level=4)
        cdf['api_name'] = body_name
        cdf['personId'] = person_data['person']['id']


        ###
        tdf = pd.DataFrame(person_data.get(body_name))
        major_columns = list(m_input.get(body_name, [])) + ['tranId']
        missing_columns = [col for col in major_columns if col not in tdf.columns]  # Identify missing columns
        tdf = tdf.assign(**{col: None for col in missing_columns})              # Add missing columns with None values
        ldf = ldf.merge(tdf, on='tranId', how='inner')
        cdf = cdf.merge(tdf, on='tranId', how='inner')
        cdf = cdf.sort_values(by='tranId')
        ldf = ldf.sort_values(by='tranId')
        ###
        print(f'\n\n\n-------------------------COMPANY_SEARCH ON {person_count + skip_persons}th person-------------')
        print(f'-------------------------COMPANY_SEARCH ON {event_count-event_max}_{event_count} events-------------')
        # cdf_cols = m_input.get(body_name, []) + ['company.name', 'franchise.name', 'franchise.branch', ]
        cdf.rename(columns={'franchise.name': 'fran.name', 'franchise.branch': 'branch', 'franchise.hqAddress': 'hq'}, inplace=True)
        if 'company.name' in cdf.columns:
            if 'company.address.full' in cdf.columns:
                print(cdf[m_input.get(body_name, []) + printout_result['cs_company']])
            else:
                print(cdf[m_input.get(body_name, []) + printout_result['cs_only_companyName']])
            print('-------------------------FRAN-------------')
            print(cdf[m_input.get(body_name, []) + printout_result['cs_fran']])
            print('-------------------------CATE-------------')
            print(cdf[m_input.get(body_name, []) + printout_result['cs_cate']])
            print()
        else:
            print(cdf)
        print('-------------------------LABEL RESULTS-------------')
        ldf_cols = ['eid'] + m_input.get(body_name, []) + ['id', 'labelClass']
        print(ldf[ldf_cols].to_string(index=False))

        lab_result = pd.concat([lab_result, ldf])
        cs_result = pd.concat([cs_result, cdf])

        # person result
        pdf = pd.json_normalize(result['data']['person']['labels'])
        pdf['api_name'] = body_name
        pdf['personId'] = person_data['person']['id']
        if len(pdf) == 0:
            print(f'NO PERSON RESULT FOUND!')
            print(pdf)
            print('-----------------')
            print(person_data.get(body_name))
            continue        
        print('-------------------------PERSON RESULTS-------------')
        pdf_cols = printout_result['person']
        print(pdf[pdf_cols].to_string(index=False))

        ps_result = pd.concat([ps_result, pdf])

    lab_result.to_csv(f'./output/{d_num}_{file_names.get(body_name).split(sep=".")[0]}_labresult.csv', encoding='utf-8-sig', index=False)
    cs_result.to_csv(f'./output/{d_num}_{file_names.get(body_name).split(sep=".")[0]}_csresult.csv', encoding='utf-8-sig', index=False)
    ps_result.to_csv(f'./output/{d_num}_{file_names.get(body_name).split(sep=".")[0]}_psresult.csv', encoding='utf-8-sig', index=False)