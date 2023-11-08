import requests
import pprint

class VisualSearchApi:
    def __init__(self):
        self.base_url = 'http://13.124.22.203:8080/application/labeler/'
        self.url = ''

    def request_api(self, body_data):
        result = requests.post(self.url, json=body_data)
        print(f'response code: {result.status_code}')
        return result.json()

class CardSearchApi(VisualSearchApi):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + 'cards/search'


class BankSearchApi(VisualSearchApi):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + 'banks/search'

class EfincSearchApi(VisualSearchApi):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + 'efinances/search'

class PersonOneApi(VisualSearchApi):
    def __init__(self):
        super().__init__()
        self.url = self.base_url + 'person/labels'


if __name__ == '__main__':
    sample_body_card = {"person": {
        "id": "userId",
        "age": 32,
        "gender": "MALE"
    },
        "cards": [
            {
                "tranId": "tdee4RiLOhT",
                "paidDtime": "20230901100000",
                "paidAmt": 4500,
                "merchantName": "GS25 광진우체국점"
            }
        ]
    }
    
    sample_body_efinc = {
    "person": {
        "id": "userId",
        "age": 32,
        "gender": "MALE"
    },
    "efinances":[
    {
        "tranId": "c9Xii4RiLOhT",
        "transMemo": "",
        "merchantName": "주식회사 카카오",
        "merchantRegno": "120-81-47521",
        "transTitle": "스트리밍클럽",
        "transAmt": 8690,
        "transDtime": "20230901100000",
        "payMethod": "01"
    }]}

    efincapi = EfincSearchApi()
    res = efincapi.request_api(body_data=sample_body_efinc)

    pprint.pprint(res)

