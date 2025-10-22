import json
from math import ceil
from typing import Iterable, Any
from datetime import datetime
from collections import OrderedDict

from scrapy import Request, Spider

class ProcoreSpider(Spider):
    name = "procore"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        # 'cookie': 'pc_geo=PK; AMCVS_FE154C895C73B0C90A495CD8%40AdobeOrg=1; s_cc=true; sa-user-id=s%253A0-4da07c36-a8fa-5ff1-40d5-9719c88867d4.AaJbRlZGS6D96B0k61tkJAnkfp%252Bi4Y7rYG0i%252BNJzw5A; sa-user-id-v2=s%253ATaB8Nqj6X_FA1ZcZyIhn1Ccj9IY.H%252FKxo8ChqN2AoM2id8oaH8cGPqrkInaibZVQkScvOgY; _fbp=fb.1.1746607563473.753699570755389381; AMCV_FE154C895C73B0C90A495CD8%40AdobeOrg=179643557%7CMCIDTS%7C20216%7CMCMID%7C10173315083141801212918769975343566118%7CMCAAMLH-1747212362%7C3%7CMCAAMB-1747212362%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCOPTOUT-1746614762s%7CNONE%7CMCSYNCSOP%7C411-20223%7CvVersion%7C5.5.0; _gid=GA1.2.1017872189.1746607565; _gcl_au=1.1.1511366193.1746607565; _mkto_trk=id:788-HIQ-636&token:_mch-procore.com-62a39fbc78d536e2eed3d2b2de77320a; FPID=FPID2.2.SBhuHjq%2Fxi32FxF5u3pkeTzSonDcIeF41LGZFgEVSz8%3D.1746607565; FPLC=78D8Zv9tMS%2Fwt3HEsXCmtoj8pvYNMbH9twbAY3FS2RmQiMapQ%2BXkd%2BwQTHHs5ZdTU%2BcXRqzicXEi2GDyN0m4%2BoKwH%2BMoyBPRHjiWlLBfK83GV%2FQ2OobeslXiMpyeZg%3D%3D; _parsely_session={%22sid%22:2%2C%22surl%22:%22https://www.procore.com/network/us/fl%22%2C%22sref%22:%22https://www.procore.com/network/us%22%2C%22sts%22:1746609730296%2C%22slts%22:1746607564027}; _parsely_visitor={%22id%22:%22pid=a9677440-67bc-455b-8854-11b2527eff0f%22%2C%22session_count%22:2%2C%22last_session_ts%22:1746609730296}; gpv_v14=www.procore.com; pc_utm=; s_sq=%5B%5BB%5D%5D; pc_fp=https://www.procore.com/network/us/fl; amp_82bb66=ynt6HZyL3BWdsyx1rRJfhs...1iql0qt9b.1iql302sh.p.0.p; gpv_pn=www.procore.com%2CAll%2COther%2CFl; gpv_v15=%2Fnetwork%2Fus%2Ffl; _rdt_uuid=1746607562816.5c479def-582b-498f-99ed-06d0ab4b1c94; sa-user-id-v3=s%253AAQAKIOQcfv93jDpueYua6aog60V6cwzeSKJXoi3DPVqxY1RwEAEYAyCf3uzABjABOgRURhDCQgSSsZZ-.cJ48owzW3jiJF9%252BbiDNvWhaHeNjBqJm%252FNgv2n6Xpyzw; _uetsid=b56855f02b1f11f09e7df37290207e31; _uetvid=b5687ab02b1f11f0b51e77d0192ce2df; _ga_DDN1X7BZGJ=GS2.1.s1746609730$o2$g1$t1746611998$j7$l0$h0; _ga_PROCORE=GS2.1.s1746609730$o2$g1$t1746611999$j0$l0$h2033214093; _ga_0W3CW2NEWP=GS2.1.s1746609730$o2$g1$t1746611999$j6$l0$h0; _ga=GA1.2.1183202900.1746607565; __cf_bm=3GXA8sLFUJXgnsYrk9.9ifeUpBzoGuEa7uBUGzAQYsU-1746612092-1.0.1.1-51hK7iJJ31y3s8v0msBYz1YI4QeIan7p0B3uS4Gu4aYeii7rGaH8Z8U3hljtZd.V.uh8mgNdvJRCTofJdPfW1xHCeEyALfk91NcBwcx3XXU',
    }
    base_api_url = "https://pcn.procore.com/api/search?state={state}&country=USA&sort=relevance&page={page}&pageSize=40"

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.states = ["fl"]

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_file = f'procore_contracters/output/procore_contractors_{timestamp}.xlsx'

        self.custom_settings = {
            'DOWNLOAD_DELAY': 1,
            'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
            'FEED_EXPORTERS': {
                'xlsx': 'scrapy_excel.ExcelItemExporter',
            },
            'FEEDS': {
                output_file: {
                    'format': 'xlsx',
                    'overwrite': False,
                }
            }
        }

    def start_requests(self) -> Iterable[Request]:
        for state in self.states:
            url = self.build_api_url(state=state, page=1)
            yield Request(url=url, callback=self.parse_pagination, headers=self.headers, cb_kwargs={'state': state})

    def build_api_url(self, state: str, page: int) -> str:
        return self.base_api_url.format(state=state, page=page)

    def parse_pagination(self, response, state: str):
        json_data = response.json()
        total_items = json_data.get('count', 0)
        total_pages = ceil(total_items / 40)

        for page_num in range(1, total_pages + 1):
            page_url = self.build_api_url(state=state, page=page_num)
            yield Request(page_url, callback=self.detail_page, headers=self.headers, dont_filter=True)

    def detail_page(self, response):
        try:
            data = json.loads(response.text)
        except Exception as e:
            self.logger.error("Could not parse JSON")
            data={}

        for contractor in data.get('results', []):
            item = OrderedDict()
            item['Name'] = contractor.get('name', '')
            item['Phone'] = contractor.get('phone')
            item['Website'] = contractor.get('website')

            address_data = contractor.get('addresses') or {}
            if isinstance(address_data, list):
                address_data = address_data[0] if address_data else {}

            item['Address'] = address_data.get('address1', '')
            item['City'] = address_data.get('city', '')
            item['State'] = address_data.get('province', '')
            item['Zip'] = address_data.get('postalCode1', '')

            slug = contractor.get('primarySlug', '')
            item['URL'] = f"https://www.procore.com/network/p/{slug}" if slug else ''

            yield item

        self.summary_data()

    def summary_data(self):



