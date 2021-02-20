import os
import tabula
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas_gbq
from flask import Flask, request, redirect, url_for
from datetime import date, timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import warnings

app = Flask(__name__)

def webscraping(url, x, folder_location):
    try:
        response = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        response.mount('http://', adapter)
        response.mount('https://', adapter)
        response = response.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        for link in soup.select("a[href$='" + x + ".pdf']"):
            # Name the pdf files using the last portion of each link which are unique in this case
            filename = os.path.join(folder_location, link['href'].split('/')[-1])
            with open(filename, 'wb') as f:
                f.write(requests.get(urljoin(url, link['href'])).content)
        if (len(soup.select("a[href$='" + x + ".pdf']")) > 0):
            return True
        else:
            return False
    except ValueError:
        return False


def download(string, dir):
    url = "https://www.gov.il/he/departments/general/daily_report" ## url to download
    # If there is no such folder, the script will create one automatically
    folder_location = str(dir)
    if (string == "today"): ## it means if we are taking the pdf file of today
        todays_date = str(date.today() - timedelta(days=1)) ## always taking the day before
        date_in_str = todays_date.replace('-', '') ## change the string date to be that pattern yyyymmdd - because it fits the pdf file name
        response = webscraping(url, todays_date, folder_location) ## if there is pdf file of today_date it download it and return TRUE
        if (response == False): ## means that there is now pdf file for today
            return False
    else:
        days_before = 2
        var = False
        while (var == False and days_before <= 14): ## trying to see who were the last pff file that uploaded before two days ago
            last_date = str(date.today() - timedelta(days=days_before))
            date_in_str = last_date.replace('-', '')
            var = webscraping(url, last_date, folder_location)
            days_before += 1

        if (days_before == 15 and var == False):
            return False
    latest_file = r"C:\Users\evyat\Desktop\Evyatar\Year1\semC\Raven\upload\download\daily_report_daily_report_" + date_in_str + ".pdf"
    df = tabula.read_pdf(str(latest_file), pages=1)  ## transforming into list of dataframes.
    return df


def add_date(df, a):  ## a function who add arow of the date.
    df = df.T #Transpose
    df['Date'] = a # sorting
    df = df.rename(columns={0: 'Total_death', 1: 'tot_fatal_acc', 2: 'death_sec_jews', 3: 'death_sec_arabs',
                            4: 'death_sec_for', 5: 'death_road_urban',
                            6: 'death_road_not_urban', 7: 'death_road_field', 8: 'total_pedestrian_deaths',
                            9: 'death_age_0_14', 10: 'death_age_15_24', 11: 'death_age_25_64', 12: 'death_age_65',
                            13: 'death_age_unknown', 14: 'total_cyclists_killed', 15: 'total_moto_killed',
                            16: 'total_young_drive_killed', 'Date' : 'Date'})
    return df


def process_and_upload(df_today, df_yesterday):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\evyat\Desktop\Evyatar\Year1\semC\Raven\upload\ravenfinal-305313-aed59a3f2b4b.json"
    ## environment variable that give us the key to upload data into the cloud
    a = date.today() - timedelta(days=1)  ##creating today date
    b = a.replace(year=2020)  ## creating the same date but in 2019

    new = df_today[0]
    new2 = df_yesterday[0]
    new = new.drop([5,15,18,20]) ##drop unimportant columns
    new2 = new2.drop([5,15,18,20])
    new2.reset_index(drop=True, inplace=True)
    new.reset_index(drop=True, inplace=True) ## after droping some columns we had to reset the indexes.

    columns_names = ['Total_death', 'tot_fatal_acc', 'death_sec_jews', 'death_sec_arabs', 'death_sec_for', 'death_road_urban',
           'death_road_not_urban', 'death_road_field', 'total_pedestrian_deaths', 'death_age_0_14',
           'death_age_15_24', 'death_age_25_64', 'death_age_65', 'death_age_unknown',
           'total_moto_killed', 'total_cyclists_killed', 'total_young_drive_killed']

    dftoday2021 = new.iloc[:, [-4]] - new2.iloc[:, [-4]]  ## to keep the change of 1 day.-
    ## In order to avoid the subtraction, the file itself must be seen, since each value in the table is a
    # cumulative amount, so in order to reach a figure of one day, it must be subtracted from the previous day.

    dftoday2021 = add_date(dftoday2021, a) ## add the date column
    dftoday2021[columns_names] = dftoday2021[columns_names].astype(float)
    ## reorder the columns to fir old yeard pattern.
    dftoday2021 = dftoday2021[['Total_death', 'tot_fatal_acc', 'death_sec_jews', 'death_sec_arabs', 'death_sec_for', 'death_road_urban',
           'death_road_not_urban', 'death_road_field', 'total_pedestrian_deaths', 'death_age_0_14',
           'death_age_15_24', 'death_age_25_64', 'death_age_65', 'death_age_unknown',
           'total_moto_killed', 'total_cyclists_killed', 'total_young_drive_killed','Date']]
    ##upload to BigQuery caracc -Dataset to table named all in ravenfinal-305313 project-id.
    pandas_gbq.to_gbq(dftoday2021, 'caracc.all', project_id='ravenfinal-305313', if_exists='append')
    print("Uploaded")


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


@app.route('/shutdown1')
def shutdown1():
    shutdown_server()
    return 'No available pdf file for today ,Server is shutting down...'


@app.route('/shutdown2')
def shutdown2():
    shutdown_server()
    return 'Job completed ,Server shutting down...'


@app.route('/')
def index():
    warnings.filterwarnings("ignore")
    path_to_download_pdf = r"C:\Users\evyat\Desktop\Evyatar\Year1\semC\Raven\upload\download" ##path do download the pdf file
    df_today_pdf_file = download("today", path_to_download_pdf) ## downloading file if exists and turn it into dataframe
    if (df_today_pdf_file == False):
        print("No pdf file for today")
        return redirect(url_for('shutdown1'))
    else:
        df_yesterday_pdf_file = download("last", path_to_download_pdf)
        process_and_upload(df_today_pdf_file, df_yesterday_pdf_file)
    return redirect(url_for('shutdown2'))

if __name__ == '__main__':

    app.run(debug=True)