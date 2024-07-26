import scrapy
from scrapy import FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.http import HtmlResponse
import pandas as pd
from collections import OrderedDict
import re
import queue


class ParivahanSpider(scrapy.Spider):
    name = 'parivahan_data'  # keyword for scrapy
    start_urls = ['https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml']  # keyword for scrapy

    def __init__(self):
        self.scraped_data = pd.DataFrame()
        self.pagination_count = 0
        self.pagination_rows = 0
        self.all_html_labels_extracted = []
        self.data_consolidation_process_count = 0
        self.data_consolidation_columns = 0
        self.response_queue = queue.Queue()

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'
    }

    years_to_scrape = [2019, 2020, 2021, 2022, 2023, 2024]
    parse_response = None

    # yield the years step by step, dedicated for ensuring synchronous operation
    def yield_year(self, parse_response, headers):
        if self.years_to_scrape:
            year = self.years_to_scrape.pop()
            print("Proceding with ", year)
            form_data = {
                'yaxisVar_input': 'Maker',
                'xaxisVar_input': 'Vehicle Category',
                'javax.faces.source': 'j_idt76',
                'javax.faces.partial.execute': '@all',
                'javax.faces.partial.render': 'VhCatg norms fuel VhClass combTablePnl groupingTable msg vhCatgPnl',
                'selectedYear_input': str(year),
                'j_idt76': 'j_idt76',
                'groupingTable:selectMonth_focus': '',
                #'groupingTable:selectMonth_input': str(year),
                'groupingTable:selectCatgType_focus': '',
                'groupingTable:selectCatgType_input': 'A',
                'groupingTable_scrollState': '0,0',
            }

            return FormRequest.from_response(parse_response, formdata=form_data, headers=headers,
                                             callback=self.prep_for_pagination, meta={"year": year}, dont_filter=True)
        else:
            print("All years scraped.")

    # entry point
    def parse(self, response):
        # start loading the payload one by one to mimic AJAX query - add Maker data as first step
        parse_count = 0
        print("PARSE EXECUTION : ", parse_count)
        parse_count += 1
        self.parse_response = response
        yield self.yield_year(response, self.headers)

    def prep_for_pagination(self, response):
        # queue implemented to scale to asynchornous scraping - future scope
        self.response_queue.put([response, response.meta["year"]])

        # first response for pagination is used to extract columns
        html_of_first_table = response.css('#groupingTable').get()
        interpret_list = pd.read_html(html_of_first_table)
        for data in interpret_list:
            if data.empty:
                # extract the columns from headers
                data.columns = data.columns.droplevel(level=0)  # dropping the extra set of multindexing
                self.data_consolidation_columns = data.columns  # capturing the column names

        # remove SNO from columns
        self.data_consolidation_columns = self.data_consolidation_columns[1:]
        yield scrapy.Request(response.url, callback=self.process_pagination, meta={"year": response.meta["year"]},
                             dont_filter=True)

    def process_pagination(self, response):
        response_year = self.response_queue.queue[0][1]
        print("Pagination Count: ", self.pagination_count, ", row number: ", str(self.pagination_rows))
        # used to check end of pagination
        has_data = True

        # check HTML response if needed for validation
        # open_in_browser(self.parse_response)

        form_data = {
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': 'groupingTable',
            'javax.faces.partial.execute': 'groupingTable',
            'javax.faces.partial.render': 'groupingTable',
            'groupingTable': 'groupingTable',
            'groupingTable_pagination': 'true',
            'groupingTable_first': '0',
            'groupingTable_rows': '25',
            'groupingTable_skipChildren': 'true',
            'groupingTable_encodeFeature': 'true',
            #'selectedYear_input': f'{response_year}',
            'groupingTable:selectMonth_focus': '',
            #'groupingTable:selectMonth_input': f'{response_year}',
            'groupingTable:selectCatgType_focus': '',
            'groupingTable:selectCatgType_input': 'A',
            'groupingTable_scrollState': '0,0',
        }

        form_data['groupingTable_first'] = str(self.pagination_rows)
        self.pagination_rows += 25 # website increases in the count of 25 rows

        # bypass pagination check if it's first page
        if self.pagination_count > 0:
            # retrieve pagination information
            response = HtmlResponse(url=response.url, body=response.body)
            # inspect_response(response, self)
            has_data = response.css("#groupingTable label::text")

        self.pagination_count += 1
        if has_data:
            for labels in response.css("#groupingTable label::text"):
                self.all_html_labels_extracted.append(str(labels))

            yield FormRequest.from_response(self.response_queue.queue[0][0], formdata=form_data, headers=self.headers,
                                            callback=self.process_pagination, dont_filter=True)

        else:
            print(f"Exiting pagination logic since end of page reached for {response_year}.")

            self.process_output(response, response_year)
            print("(Optional) for asynchronous application in future: Retrieving object from queue ", self.response_queue.get(), response_year)
            self.pagination_rows = 0
            self.pagination_count = 0

            yield self.yield_year(self.parse_response, self.headers)

    def process_output(self, response, year):
        try:
            self.data_consolidation_process_count += 1
            all_labels = self.all_html_labels_extracted
            pattern = r"\b\d+(?:[,.]\d+)*\b"
            maker_sales = OrderedDict() # orderedDict to make sure the data order is maintained
            sales = []
            previous_maker_index = None

            # logic to process data
            for index, value in enumerate(all_labels):
                # found a "maker" - if current value is alphanumeric
                if not re.fullmatch(pattern, value):
                    if sales:
                        maker_sales[all_labels[previous_maker_index]] = sales[:-1]
                        sales.clear()
                    previous_maker_index = index

                # if current value is only digits
                else:
                    if previous_maker_index:
                        sales.append(value)

                # check for the last iteration
                if index + 1 == len(all_labels):
                    maker_sales[all_labels[previous_maker_index]] = sales[:]
                    sales.clear()

            for key, value in maker_sales.items():
                if len(value) != 17:
                    print("ERRORNEOUS VALUE (if any): ", key, value)

            self.scraped_data = pd.DataFrame.from_dict(maker_sales, orient='index')
            self.scraped_data.reset_index(inplace=True)
            self.scraped_data.columns = self.data_consolidation_columns
            self.scraped_data.insert(0, column="year", value=year)
            self.scraped_data.to_csv(f"{year}_data.csv", index=False)
            success = True
        except:
            success = False
