import os
import re
import json
import requests
import psycopg2
import pandas as pd
import paho.mqtt.client as mqtt
from bs4 import BeautifulSoup
from datetime import datetime


class InsiderTrading:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    mqtt_user = None
    mqtt_password = None
    mqtt_host = None
    db_host = None
    db_user = None
    db_password = None
    db_database = None
    debug = False

    def __init__(self, config, debug=False) -> None:
        self.config = config
        if debug:
            print("::InsiderTrading")
            print(config)
        self.mqtt_user = config["MQTT_USER"]
        self.mqtt_password = config["MQTT_PASSWORD"]
        self.mqtt_host = config["MQTT_HOST"]
        self.db_host = config['HOST']
        self.db_user = config['USER']
        self.db_password = config['PASSWORD']
        self.db_database = config['DATABASE']
        self.debug = debug


    def clean_explanation(self, data):
        return re.findall(r"\(([^\)]+)\)", str(data.strip()))

    def clean_data(self, data):
        data = re.sub(r"\([^)]*\)|\$|,", "", str(data.strip()))
        return data

    def mqtt_connect(self):
        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", reason_code)

        client_id = "insider_trading"
        self.mqtt_client = mqtt.Client(
            client_id=client_id, callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.username_pw_set(
            self.mqtt_user, self.mqtt_password
        )
        self.mqtt_client.connect(self.mqtt_host, 1883, 60)

    def mqtt_publish(self, topic, message):
        # publish.single("insider_trading/status", payload=json.dumps(message), qos=2, hostname=self.config["MQTT_HOST"], port=1883)
        self.mqtt_client.publish(f"insider_trading/{topic}", message)

    def db_connection(self):
        try:
            conn = psycopg2.connect(
                dbname=self.db_database,
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port="5432",
            )
        except Exception as e:
            self.mqtt_publish("error", "[db_connection] Error connecting to the database")
            conn = None
        return conn

    def query_db(self, sql):
        conn = self.db_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    result = cursor.fetchall()
            except Exception as e:
                self.mqtt_publish("error", "[query_db] Error executing database query")
                result = None
            finally:
                conn.close()
        else:
            result = None
        return result

    def insert_table(self, table_name, data):
        conn = self.db_connection()
        cols = {
            "latest_filings": "html_url,insider_name,accepted_datetime,fill_date,file_code,film_no,comment",
            "insider_trading": "fill_date,accepted_datetime,ticker,company_name,insider_name,title,trade_type,exercise_price,price,qty,qty_reported,date_exercisable,values,form4_url,explanation,latest_filing_id",
        }
        if conn:
            try:
                # Dynamic SQL insert statement, list of column values depends on list data length
                sql_insert = f"""INSERT INTO {table_name} ({cols[table_name]}) VALUES ({', '.join(['%s'] * len(data[0]))})"""
                with conn.cursor() as cursor:
                    if isinstance(data, list) and len(data) > 0:
                        cursor.executemany(sql_insert, data)
                    else:
                        cursor.execute(sql_insert, data)
                conn.commit()
            except Exception as e:
                self.mqtt_publish("error", "[insert_table] Error inserting data into the database")
            finally:
                conn.close()

    def get_latest_filings(self):
        try:
            self.mqtt_publish("agent", "[get_latest_filings] Fetching last saved data(html url) from Database")
            last_html = self.query_db(
                "SELECT html_url FROM latest_filings ORDER BY accepted_datetime DESC LIMIT 1;"
            )
            # If last html_url present in the table or empty table, result = Type list
            if isinstance(last_html, list):
                # only 1 item, max should be returned
                if len(last_html) == 1:
                    last_html = last_html[0][0]
                elif len(last_html) == 0:
                    last_html = ""
            self.mqtt_publish("agent", f"[get_latest_filings] last_html = {last_html}")
        except Exception as e:
            self.mqtt_publish("error", "[get_latest_filings] Error while fetching the latest filings")
            last_html = None
        latest_filing_data = []
        if last_html is not None:
            all_data_fetched = False
            start = 0
            self.mqtt_publish("agent", f"[get_latest_filings] all_data_fetched = {all_data_fetched}, start = {start} - Fetching latest filings from")
            # Condition to fetch number of pages till last data (html) already saved
            # while not all_data_fetched and start < 100:
            while not all_data_fetched:
                sec_edgar_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&&type=4&owner=only&count=40&start={start}"
                self.mqtt_publish("agent", f"[get_latest_filings] Page {int(start/40+1)}, {sec_edgar_url}")
                response = requests.get(sec_edgar_url, headers=self.headers)
                soup = BeautifulSoup(response.content, "html.parser")
                try:
                    filings_table = soup.div.find_all("table", recursive=False)[1]
                    filings_table = filings_table.find_all("tr")[1:]
                    # make pair of table rows, each table row pair belongs to one filing
                    filings_table_row_pairs = list(
                        zip(filings_table[::2], filings_table[1::2])
                    )
                    self.mqtt_publish("agent", f"[get_latest_filings] Found {len(filings_table_row_pairs)} rows in page : {start+1}")
                    for item in filings_table_row_pairs:
                        # item = filings_table_row_pairs[0]
                        description = item[0].text.strip()
                        comment = item[1].find_all("td")[2].text.strip()
                        html = "https://www.sec.gov" + item[1].a["href"]
                        Accepted_Date = item[1].find_all("td")[3].text[:10]
                        Accepted_Time = item[1].find_all("td")[3].text[10:]
                        Accepted_DateTime = datetime.strptime(
                            f"{Accepted_Date} {Accepted_Time}", "%Y-%m-%d %H:%M:%S"
                        )
                        Filing_Date = item[1].find_all("td")[4].text
                        try:
                            File = item[1].find_all("td")[5].text.split("\n")[0]
                            Film_No = int(item[1].find_all("td")[5].text.split("\n")[1])
                        except:
                            File = None
                            Film_No = None
                        if html != last_html:
                            self.mqtt_publish("agent", f"[get_latest_filings] row {filings_table_row_pairs.index(item)}:New Data, html = {html}")
                            latest_filing_data.append(
                                (
                                    html,
                                    description,
                                    Accepted_DateTime,
                                    Filing_Date,
                                    File,
                                    Film_No,
                                    comment,
                                )
                            )
                        elif html == last_html:
                            self.mqtt_publish("agent", f"[get_latest_filings] row {filings_table_row_pairs.index(item)}: Reached last saved data,  html = {html}")
                            all_data_fetched = True
                            break
                    # todo: remove for all pages
                    # all_data_fetched = True
                except Exception as e:
                    self.mqtt_publish("error", f"[get_latest_filings] Except block, Error in Fetching : {sec_edgar_url}, start: {start}, html: {html}, last_html: {last_html}")
                    all_data_fetched = True
                start += 40
        else:
            self.mqtt_publish("error", "[get_latest_filings] ELSE: Error fetching last saved html url from Database")
        return latest_filing_data

    def parse_to_float(self, row, pos):
        val = self.clean_data(row.find_all("td")[pos].text.strip())
        return 0 if val == "" else float(val)

    def parse_to_date(self, data):
        date_str = re.findall(r"([0-9]{2}\/[0-9]{2}\/[0-9]{4})", str(data.strip()))
        if len(date_str) > 0:
            return datetime.strptime(date_str[-1], "%m/%d/%Y").date()
        

    def extract_explanations(self, row, pos, explanation_table):
        explanation = []
        explanation_positions = self.clean_explanation(
            row.find_all("td")[pos].text.strip()
        )
        if len(explanation_positions) > 0:
            for explanation_position in explanation_positions:
                explanation.append(
                    explanation_table.find_all("td")[
                        int(explanation_position)
                    ].text.strip()
                )
        return explanation

    def get_filing_details(self, latest_filing_id, index_html_url: str):
        insider_trading_data = []
        index_response = requests.get(index_html_url, headers=self.headers)
        if index_response.status_code == 200:
            try:
                index_soup = BeautifulSoup(index_response.content, "html.parser")
                accepted_datetime = (
                    index_soup.find("div", class_="formGrouping")
                    .find_all("div", class_="info")[1]
                    .text
                )
                index_table = index_soup.find("table")
                form4_url = "https://www.sec.gov" + index_table.a["href"]
                self.mqtt_publish("agent", f"[get_filing_details] form4_url = {form4_url}")
                form4_response = requests.get(form4_url, headers=self.headers)
                if form4_response.status_code == 200:
                    try:
                        form4_soup = BeautifulSoup(
                            form4_response.content, "html.parser"
                        )
                        all_tables = form4_soup.body.find_all("table", recursive=False)
                        company_data = all_tables[1]
                        non_derivative_table = all_tables[2]
                        derivative_table = all_tables[3]
                        explanation_table = all_tables[4]
                        company_data = company_data.tr.find_all("td", recursive=False)
                        insider_name = company_data[0].td.text
                        company_name = company_data[1].a.text
                        ticker = company_data[1].find_all("span")[1].text
                        director = company_data[2].find_all("td")[0].text
                        owner_10 = company_data[2].find_all("td")[2].text
                        officer = company_data[2].find_all("td")[4].text
                        other = company_data[2].find_all("td")[6].text
                        if officer or other:
                            title = company_data[2].find_all("td")[8].text.strip()
                            if title == "":
                                title = company_data[2].find_all("td")[9].text.strip()
                            if title == "See Remarks":
                                title = (
                                    all_tables[4]
                                    .find("tr", string="Remarks:")
                                    .next_sibling.next_sibling.text
                                )
                        elif director and owner_10:
                            title = "Director, 10% Owner"
                        elif director:
                            title = "Director"
                        elif owner_10:
                            title = "10% Owner"
                        # Non-Derivative table
                        if non_derivative_table.tbody:
                            trade_type_mapping = {"A": "Buy", "D": "Sale"}
                            security_table = non_derivative_table.tbody.find_all("tr")
                            # row = security_table[0]
                            for row in security_table:
                                fill_date = self.clean_data(row.find_all("td")[1].text)
                                if fill_date != "":
                                    qty             = self.parse_to_float(row, 5)
                                    price           = self.parse_to_float(row, 7)
                                    qty_reported    = self.parse_to_float(row, 8)
                                    trade_type      = trade_type_mapping.get(
                                        self.clean_data(
                                            row.find_all("td")[6].text.strip()
                                        ),
                                        None,
                                    )
                                    explanation     = self.extract_explanations(
                                        row, 5, explanation_table
                                    ) + self.extract_explanations(
                                        row, 7, explanation_table
                                    )
                                    values          = round(qty * price)
                                    insider_trading_data.append(
                                        (
                                            self.parse_to_date(fill_date),
                                            accepted_datetime,
                                            ticker,
                                            company_name,
                                            insider_name,
                                            title,
                                            trade_type,
                                            0,
                                            price,
                                            qty,
                                            qty_reported,
                                            "",
                                            values,
                                            form4_url,
                                            "\n".join(explanation),
                                            latest_filing_id,
                                        )
                                    )
                        # Derivative Table
                        if derivative_table.tbody:
                            derivative_table = derivative_table.tbody.find_all("tr")
                            # row = derivative_table[0]
                            for row in derivative_table:
                                fill_date = self.clean_data(
                                    row.find_all("td")[2].text.strip()
                                )
                                if fill_date != "":
                                    # trade_type = "Option Exercise"
                                    qty                 = self.parse_to_float(row, 6)
                                    price               = self.parse_to_float(row, 12)
                                    qty_reported        = self.parse_to_float(row, 13)
                                    explanation         = self.extract_explanations(
                                        row, 1, explanation_table
                                    ) + self.extract_explanations(
                                        row, 8, explanation_table
                                    )
                                    exercise_price      = self.parse_to_float(row, 1)
                                    trade_type          = row.find_all("td")[0].text.strip()
                                    date_exercisable    = self.parse_to_date(
                                        row.find_all("td")[8].text.strip()
                                    )
                                    values              = round(qty * price)
                                    insider_trading_data.append(
                                        (
                                            self.parse_to_date(fill_date),
                                            accepted_datetime,
                                            ticker,
                                            company_name,
                                            insider_name,
                                            title,
                                            trade_type,
                                            exercise_price,
                                            price,
                                            qty,
                                            qty_reported,
                                            date_exercisable,
                                            values,
                                            form4_url,
                                            "\n".join(explanation),
                                            latest_filing_id,
                                        )
                                    )
                                    self.mqtt_publish("agent", f"[get_filing_details] {fill_date},{accepted_datetime},{ticker},{company_name},{insider_name},{title},{trade_type},{price},{qty},{values}")

                        return insider_trading_data
                    except Exception as e:
                        self.mqtt_publish("error", f"[get_filing_details] Error while fetching form4_url = {form4_url}:")
                else:
                    self.mqtt_publish("error", f"[get_filing_details] Error in request {form4_url} - {form4_response.status_code} {form4_response.text}")
            except requests.exceptions.RequestException as re:
                self.mqtt_publish("error", f"[get_filing_details] Request Exception while fetching {index_html_url}:")
        return insider_trading_data

    def save_to_excel(self, data: list, excel_file=r"insider_trading.xlsx"):
        headers = [
            "Fill Date",
            "Time",
            "Ticker",
            "Company",
            "Insider Name",
            "Title",
            "Trade Type",
            "Price",
            "QTY",
            "Value",
            "SEC URL",
        ]
        data_df = pd.DataFrame(data, columns=headers)
        try:
            # Check if the file exists or not
            file_exists = os.path.isfile(excel_file)
            # Try to append data to an existing Excel file or create a new one
            if file_exists:
                # get last row postion in existing excel file
                last_row = pd.read_excel(excel_file).index.stop + 1
                with pd.ExcelWriter(
                    excel_file, mode="a", if_sheet_exists="overlay"
                ) as writer:
                    data_df.to_excel(
                        writer, startrow=last_row, index=False, header=False
                    )
                    self.mqtt_publish("agent", f"[save_to_excel] Data appended to file {excel_file}.")
            else:
                with pd.ExcelWriter(excel_file, mode="w") as writer:
                    data_df.to_excel(writer, index=False)
                    self.mqtt_publish("error", f"[save_to_excel] New file {excel_file} created and data saved.")
        except PermissionError:
            self.mqtt_publish("error", f"[save_to_excel] Error: Permission denied while trying to access {excel_file}.")
        except Exception as e:
            self.mqtt_publish("error", f"[save_to_excel] An error occurred: {e}")

    def run(self):
        self.mqtt_connect()
        self.mqtt_client.loop_start()
        self.mqtt_publish("status", f'[run] {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} : Starting Main Function')
        latest_filing_data = self.get_latest_filings()
        if latest_filing_data:
            self.mqtt_publish("agent", f"[run] Inserting latest_filings Data in Table latest_filing, {len(latest_filing_data)} rows")
            self.insert_table("latest_filings", latest_filing_data)
            html_urls = [item[0] for item in latest_filing_data]
            insider_trading_data = []

            self.mqtt_publish("agent", "[run] Fetching Form4 Data")
            for position, html_url in enumerate(html_urls, start=1):
                self.mqtt_publish("status", f"[run] processing {position}/{len(html_urls)}")
                result = self.query_db(f"select id from latest_filings where html_url = '{html_url}'")
                if len(result) > 0:
                    latest_filing_id = result[0][0]
                    insider_trading_data = self.get_filing_details(latest_filing_id, html_url)
                    if insider_trading_data:
                        self.mqtt_publish("agent", '[run] Inserting Form4 data into insider_trading table')
                        self.insert_table("insider_trading", insider_trading_data)
                        # save_to_excel(insider_trading_data)
            self.mqtt_publish("status", f"[run] done {len(html_urls)}")
        else:
            self.mqtt_publish("status", "[run] no new data")
        self.mqtt_client.loop_stop()

    def process_fillings(self):
        self.mqtt_connect()
        self.mqtt_client.loop_start()
        rows = self.query_db(
            "SELECT id, html_url FROM latest_filings lf WHERE lf.id NOT IN (SELECT DISTINCT latest_filing_id FROM insider_trading) order by accepted_datetime desc"
        )
        for position, row in enumerate(rows, start=1):
            self.mqtt_publish("status", f"[process_fillings] processing filing {position}/{len(rows)}")
            insider_trading_data = self.get_filing_details(
                latest_filing_id=row[0], index_html_url=row[1]
            )
            if insider_trading_data:
                self.mqtt_publish("agent", "[process_fillings] Inserting Form4 data into insider_trading table")
                self.insert_table("insider_trading", insider_trading_data)
                # save_to_excel(insider_trading_data)
        self.mqtt_client.loop_stop()

    def process_filing_details(self, latest_filing_id, index_html_url):
        self.mqtt_connect()
        self.mqtt_client.loop_start()

        insider_trading_data = self.get_filing_details(
            latest_filing_id=latest_filing_id, index_html_url=index_html_url
        )
        if insider_trading_data:
            self.mqtt_publish("agent", "[process_fillings] Inserting Form4 data into insider_trading table")
            self.insert_table("insider_trading", insider_trading_data)
        self.mqtt_client.loop_stop()


# from dotenv import dotenv_values

# path_to_env = "../.env"
# config = dotenv_values(path_to_env)
# print(config)


# it = InsiderTrading(config)
# # it.run()
# it.process_fillings()
# # # it.process_filing_details(192, 'https://www.sec.gov/Archives/edgar/data/1065715/000114036124018434/0001140361-24-018434-index.htm')
# # # it.process_filing_details(96, 'https://www.sec.gov/Archives/edgar/data/1947633/000121390024030948/0001213900-24-030948-index.htm')
# # # it.process_filing_details(98, 'https://www.sec.gov/Archives/edgar/data/1947645/000121390024030947/0001213900-24-030947-index.htm')
# # # it.process_filing_details(99, 'https://www.sec.gov/Archives/edgar/data/1830081/000121390024030947/0001213900-24-030947-index.htm')
# # # it.process_filing_details(100, 'https://www.sec.gov/Archives/edgar/data/1947980/000121390024030946/0001213900-24-030946-index.htm')
