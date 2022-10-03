import requests


# так не бажано:
requests.get(
    url='https://example.com?date=2022-09-30&page=3'
)

# краще так:
requests.get(
    url='https://example.com',
    params={
        'date': '2022-09-30',
        'page': 3,
    }
)
