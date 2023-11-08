import pandas as pd
import datetime
import random
import numpy as np


def get_bank(search_txt, person_id='person_temp', tran_id='test_one', amt=1000, trans_type=2,
             age=None, gender=None, org_name=None, trans_class=None):
    unique_ = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    use_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    rand_ = str(random.randint(19, 69))

    person_rec = {'id': person_id,
                  'age': age,
                  'gender': gender
                  }
    tran_rec = {'tranId': f'{tran_id}_{unique_}_{rand_}',
                'orgName': org_name,
                'transType': trans_type,
                'transClass': trans_class,
                'transMemo': search_txt,
                'transAmt': amt,
                'transDtime': use_time
                }

    body = {'person': {x: y for x, y in person_rec.items() if y},
            'banks': [{x: y for x, y in tran_rec.items() if y}]
            }
    return body


def get_card(search_txt, person_id='person_temp', tran_id='test_one', amt=1000,
             age=None, gender=None, brno=None):
    unique_ = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    use_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    rand_ = str(random.randint(19, 69))

    person_rec = {'id': person_id,
                  'age': age,
                  'gender': gender
                  }

    tran_rec = {'tranId': f'{tran_id}_{unique_}_{rand_}',
                'merchantRegno': brno,
                'merchantName': search_txt,
                'paidAmt': amt,
                'paidDtime': use_time
                }
    body = {'person': {x: y for x, y in person_rec.items() if y},
            'cards': [{x: y for x, y in tran_rec.items() if y}]
            }
    return body


def get_efin(search_txt, person_id='person_temp', tran_id='test_one', amt=1000,
             age=None, gender=None, brno=None, trans_memo=None, trans_title=None, pay_method='01'):
    unique_ = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    use_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    rand_ = str(random.randint(19, 69))

    person_rec = {'id': person_id,
                  'age': age,
                  'gender': gender
                  }

    tran_rec = {'tranId': f'{tran_id}_{unique_}_{rand_}',
                'merchantRegno': brno,
                'merchantName': search_txt,
                'transMemo': trans_memo,
                'transTitle': trans_title,
                'payMethod': pay_method,
                'transAmt': amt,
                'transDtime': use_time
                }
    body = {'person': {x: y for x, y in person_rec.items() if y},
            'efinances': [{x: y for x, y in tran_rec.items() if y}]
            }
    return body

def get_person(date_from, date_to, userId):
    body = {
        "fromDate": date_from,
        "toDate": date_to,
        "userId": userId
    }
    return body

def load_df(fp, skip_rows=0, dtype_mapping={}):
    print()
    print(f'...loading {fp}....')
    if fp[-3:] == 'txt':
        delim = '|'
    else:
        delim = ','
    if skip_rows > 1:
        header = pd.read_csv(fp, encoding='utf-8-sig', delimiter=delim, nrows=0).columns
        df = pd.read_csv(fp, encoding='utf-8-sig', delimiter=delim, header=None, skiprows=skip_rows, dtype=dtype_mapping)
        df.columns = header
        return df
    return pd.read_csv(fp, encoding='utf-8-sig', delimiter=delim, dtype=dtype_mapping)


def yield_chunks(lst, chunk_size):
    """Yield successive chunk_size chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def make_demo():
    genders = ['MALE', 'FEMALE']
    rec = {'age':  random.randint(19, 69), 'gender': genders[random.randint(0, 1)]}
    return rec


def get_file(fp, api_name='banks', person_max=100, event_max=10000, add_demo=False,
             skip_rows=0, skip_persons=0, dtype_mapping={}):
    """

    :param fp: file path
    :param api_name: api name
    :param person_max: number of persons you want to run the api.
    :param event_max: maximum number of events per person
    :param add_demo: If true, gender and age will be added randomly on person information
    :param skip_rows: number of rows to skip in the csv file
    :param skip_persons: number of persons to skip
    :return:
    """
    df = load_df(fp, skip_rows, dtype_mapping)
    try:
        df['payMethod'] = df['payMethod'].astype(str).str.zfill(2)
    except:
        pass
        
    print('...loading complete!')
    print()
    df = df.groupby('id')
    person_count = 0
    for person_id, trans in df:
        if person_count < skip_persons:
            person_count += 1
            continue        # skip n persons.
        person_rec = {'id': person_id}
        if add_demo:
            person_rec.update(make_demo())
        ############# merchantName must exist in 'cards'
        if api_name == 'cards':
            trans = trans.dropna(subset=['merchantName'])
        tran_recs = trans.drop(columns=['id']).where(pd.notna(trans), '').to_dict('records')

        ###### ensure uniqueness of transaction id
        for index, item in enumerate(tran_recs):
            item['tranId'] = f"{str(person_count)}_{str(index)}_{item['tranId']}"  # to ensure uniqueness of tran id
        ##########################################
        #### ensures search_txt is not none

        for i in range(0, len(tran_recs), event_max):
            yield {'person': person_rec, api_name: tran_recs[i:i + event_max]}
        person_count += 1
        if person_count == person_max + skip_persons:
            break


if __name__ == '__main__':
    for h in get_file('data/test_all_bank.txt', api_name='banks', person_max=10):
        print(h)


