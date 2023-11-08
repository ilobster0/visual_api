import data_maker as d
from visual_search import BankSearchApi, CardSearchApi, EfincSearchApi, PersonOneApi
import pprint


def print_input(input_, api_name=''):
    print('')
    print('INPUT  ------------------------------------')
    pprint.pprint(input_)
    print()
    print(f'RESULT OF {api_name}  -----------------------')


if __name__ == '__main__':
    b = BankSearchApi()
    c = CardSearchApi()
    e = EfincSearchApi()
    p = PersonOneApi()

    search_txt = '청약'
    trans_class = '기타'
    person_id = 'test_bang_000003'
    # 삼성생0001
    # 이마트 구로점
    # 코리아세븐 성북정릉동아점
    # GS25낙성대역점
    brno = '203-13-31683'

    # b_input = d.get_bank(search_txt=search_txt, trans_type=2, person_id='test_one', trans_class='카드', trans_amt=1000)

    input_data = d.get_bank(search_txt=search_txt, trans_class=trans_class,trans_type=2)
    print_input(input_data, 'BANK')
    result = b.request_api(input_data)
    result_tran = result['data']['transaction']
    pprint.pprint(result_tran)

    # input_data = d.get_card(search_txt=search_txt, brno=brno)
    # print_input(input_data, 'CARD')
    # result = c.request_api(input_data)
    # result_tran = result['data']['transaction']
    # pprint.pprint(result_tran)

    # input_data = d.get_efin(search_txt)
    # print_input(input_data, 'EFINC')
    # result = e.request_api(input_data)
    # result_tran = result['data']['transaction']
    # pprint.pprint(result_tran)

    # personid = '+0lAY1WdYFyopc2YqgqcpmruSiqlA7xODlPI1+VocoQ6qtRFOL63qsu/xSvwXq811a/nZ7PxTmzrwNh9BgIShA=='
    # input_data = d.get_person('20220901','20221001', personid)
    # result = p.request_api(input_data)
    # pprint.pprint(result)
    