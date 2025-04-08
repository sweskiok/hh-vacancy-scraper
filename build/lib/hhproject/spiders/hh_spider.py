import scrapy
import json
import datetime
from urllib.parse import urlencode

class HhVacancySpider(scrapy.Spider):
    name = "hh_vacancy"
    allowed_domains = ["api.hh.ru"]

    # Список ID муниципальных округов Иркутской области
    areas = [
        7419, 7449, 4854, 1125, 1126, 7340, 7377, 1127, 11487, 4855, 7420, 6249, 4856, 7446, 1128, 1129,
        7442, 4857, 4858, 1130, 7393, 6657, 6746, 7351, 1131, 4864, 7408, 11572, 4859, 7311, 1132,
        4860, 4861, 7400, 3716, 1133, 7386, 35, 7418, 7362, 4862, 4863,
        7324, 1134, 4865, 4866, 7459, 4867, 4868, 7329, 4869, 4870, 7431, 4871,
        4872, 4873, 4874, 1135, 4875, 4876, 4877, 4878, 4879, 7394,
        11189, 4880, 7321, 7378, 4881, 1136, 1137, 7292, 1138, 6519, 4882, 4883, 1139,
        7413, 4884, 7361, 1140, 5895, 4885, 7395, 4886, 1141, 1142,
        1143, 217, 4887, 7322, 7387, 4888, 4891, 1144,
        4889, 1145, 7414, 7350, 7404, 7289, 4890, 7349
    ]

    download_delay = 1.0

    def start_requests(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        today = datetime.date.today()
        for area in self.areas:
            date_from = today - datetime.timedelta(days=7)
            date_to = today
            yield from self.request_with_dates(area, date_from, date_to, headers)

    def request_with_dates(self, area, date_from, date_to, headers):
        total_pages = 20  # Максимум 20 страниц (ограничение API)
        for page in range(total_pages):
            params = {
                "area": area,
                "per_page": 100,
                "page": page,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat()
            }
            url = f"https://api.hh.ru/vacancies?{urlencode(params)}"
            yield scrapy.Request(url, headers=headers, meta={"area": area, "date_from": date_from, "date_to": date_to})

    def parse(self, response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch data: {response.url}, status code: {response.status}")
            return

        try:
            data = response.json()
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response: {response.url}")
            return

        vacancies = data.get("items", [])
        total_found = data.get("found", 0)

        if not vacancies:
            self.logger.warning(f"No vacancies found for URL: {response.url}")
            return

        for vacancy in vacancies:
            vacancy_id = vacancy.get("id")
            if not vacancy_id:
                self.logger.error(f"Missing vacancy ID in response: {vacancy}")
                continue

            details_url = f"https://api.hh.ru/vacancies/{vacancy_id}"
            yield scrapy.Request(url=details_url, callback=self.parse_vacancy, errback=self.errback_httpbin,
                                 headers=response.request.headers, dont_filter=False)

        # Разделяем временной диапазон, если количество вакансий >= 2000
        area = response.meta["area"]
        date_from = response.meta["date_from"]
        date_to = response.meta["date_to"]

        if total_found >= 2000:
            new_step = max(1, (date_to - date_from).days // 2)
            new_date_to = date_from
            new_date_from = new_date_to - datetime.timedelta(days=new_step)
            yield from self.request_with_dates(area, new_date_from, new_date_to, response.request.headers)
        else:
            new_date_to = date_from
            new_date_from = new_date_to - datetime.timedelta(days=7)
            if new_date_from.year >= 2005 and total_found > 0:
                yield from self.request_with_dates(area, new_date_from, new_date_to, response.request.headers)

    def errback_httpbin(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(f"Failure details: {failure.value}")

    def parse_vacancy(self, response):
        if response.status != 200:
            self.logger.error(f"Failed to fetch vacancy details: {response.url}, status code: {response.status}")
            return

        try:
            vacancy = response.json()
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response for vacancy: {response.url}")
            return

        item = {
            "name": vacancy.get("name"),
            "salary": vacancy.get("salary"),
            "published_at": vacancy.get("published_at"),
            "description": vacancy.get("description"),
            "experience": vacancy.get("experience", {}).get("name"),
            "employment": vacancy.get("employment", {}).get("name"),
            "schedule": vacancy.get("schedule", {}).get("name"),
            "key_skills": [skill.get("name") for skill in vacancy.get("key_skills", [])],
            "city": vacancy.get("area", {}).get("name"),
            "employer": vacancy.get("employer", {}).get("name"),
        }

        # Запись данных в файл
        #with open("vacancies_with_date5.json", "a", encoding="utf-8") as f:
        #    f.write(json.dumps(item, ensure_ascii=False) + "\n")
        yield item