import os
import asyncio
import random
import time
from datetime import datetime, timedelta
import re
import json
from typing import List

import numpy as np
import pandas as pd
from pyppeteer import launch
from pyppeteer.browser import Browser
from pyppeteer_stealth import stealth
from tokenizers import Tokenizer

# function to remove html tags from text
def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)


# function finds the datetime of the first archived snapshot of a url after a date
# urls and dates are passed as iterables for efficiency with async
def archive_dates(urls: pd.Series, dates: pd.Series):

    # async function to open a browser
    async def _open_browser():

        return await launch()

    # async function to find archive datetime
    async def _get_archive_date(url, date, browser: Browser):

        # error handling
        try:

            # open page
            page = await browser.newPage()

            # use stealth to stop bot detection
            await stealth(page)

            # follow url
            await page.goto(
                f"http://web.archive.org/cdx/search/cdx?url={url}*&output=txt&limit=1&from={date}",
                {'timeout': 120*1000}
            )

            # get the html
            content = await page.content()

            # close page for memory
            await page.close()

            # take out html tags
            content = remove_html_tags(content)

            # get isotime
            dt = content.split(' ')[1]

            # check if dt is a datetime (raises error otherwise)
            datetime.strptime(dt, '%Y%m%d%H%M%S')

            return dt

        # print url and error message and return an empty string
        except Exception as e:

            print(f"http://web.archive.org/cdx/search/cdx?url={url}*&output=txt&limit=1&from={date}", e)

            return ''

    # gather the results
    async def _gather_results(urls_in, dates_in):

        gathered = await asyncio.gather(
            *[_get_archive_date(url, date, browser) for url, date in zip(urls_in, dates_in)]
        )

        return gathered

    # read json file
    dict_path = "data/archive_dates.json"
    if os.path.exists(dict_path):
        with open(dict_path) as file:
            dates_dict = json.load(file)
    else:
        dates_dict = {}

    # make a list of urls that are not in the dictionary
    lookup_urls = [(url, date) for url, date in zip(urls, dates) if url not in dates_dict.keys() or
                   date not in dates_dict[url].keys()]

    # save new snapshot datetimes to the archive_dates dictionary
    if len(lookup_urls) > 0:

        # separate dates and urls
        lookup_dates = [i[1] for i in lookup_urls]
        lookup_urls = [i[0] for i in lookup_urls]

        # create browser
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        browser = loop.run_until_complete(_open_browser())

        # list to populate with results
        snapshot_dates = []

        # batch size. we rate limit to one batch per minute.
        n = 15

        # iterate over batches
        for i in range(0, len(lookup_urls), n):

            # record time at start
            t0 = time.time()

            # search for dates with pyppeteer
            snapshot_dates += loop.run_until_complete(_gather_results(lookup_urls[i:i+n], lookup_dates[i:i+n]))

            # add this batch to the dict and save as json
            for url, lookup_date, snapshot_date in zip(
                lookup_urls[i:i+n], lookup_dates[i:i+n], snapshot_dates[i:i+n]
            ):
                # if this url hasn't been looked up before
                if url not in dates_dict.keys():
                    dates_dict[url] = {lookup_date: snapshot_date}

                # or if another date had already been saved for this url
                else:
                    dates_dict[url][lookup_date] = snapshot_date

            # save the updated dictionary
            with open(dict_path, "w") as file:
                json.dump(dates_dict, file, indent=4)

            # time at end
            t1 = time.time()

            # wait if a minute hasn't passed yet so that we don't get blocked
            if t1 < t0 + 60:
                time.sleep(t0 + 60 - t1)

    # make list of dates to return from dates_dict
    snapshot_dates = [
        dates_dict[url][date]
        if url in dates_dict.keys() and date in dates_dict[url].keys()
        else ''
        for url, date in zip(urls, dates)
    ]

    return snapshot_dates


# get popular websites from majestic_million.csv
def get_domains(num_domains: int) -> pd.DataFrame:

    # filter out the domains that are in the dict from the dataframe, so we dont have the reitereate on what we dont have

    # read csv and return it
    return pd.read_csv("data/majestic_million.csv", usecols=['Domain'], nrows=num_domains)


# add random dates to those websites and check that they correspond to an actual archive datetime
def get_dates(df: pd.DataFrame, start_date: datetime, end_date: datetime):

    # set random seed
    random.seed(7)

    # random dates for each website
    df["date"] = [
        (start_date + (end_date - start_date) * random.random()) for i in range(df.size)
    ]

    # reformat datetimes
    df["date"] = [dt.strftime("%Y%m%d") for dt in df["date"]]

    # fix datetimes to an actual snapshot in the way back machine
    df["date"] = archive_dates(df['Domain'], df['date'])

    # get rid of rows that we couldn't find a date for
    df = df[df["date"] != '']

    return df


# @task
def get_content(df: pd.DataFrame):

    # async function to open a browser
    async def _open_browser():

        return await launch()

    async def _get_content(url, browser):

        try:

            # open page
            page = await browser.newPage()

            # use stealth to stop bot detection
            await stealth(page)

            # follow url
            await page.goto(
                url, {'timeout': 120*1000}
            )

            # get the html
            content = await page.content()

            # close page for memory
            await page.close()

        # error handling
        except Exception as e:

            print(url, str(e))

            content = ''

        # save to local file for next time
        path = os.path.join('data', 'html', url.replace('/', '_').replace(':', '_'))[:255]

        with open(path, "w") as file:
            file.write(content)

        return content

    async def _gather_results(urls):
        return await asyncio.gather(*[_get_content(url, browser) for url in urls])

    # add url column
    df['url'] = "https://web.archive.org/web/" + df['date'] + "id_/" + df['Domain']

    # add file_names for saving locally
    df['file_name'] = [url.replace('/', '_').replace(':', '_') for url in df['url']]

    # create an empty df column for content
    df['content'] = ''

    # read contents from disc if the file exists
    # iterate over dataframe rows
    for ind in df.index:

        if df['file_name'][ind] in os.listdir('data/html'):

            path = os.path.join('data', 'html', df['file_name'][ind])[:255]

            with open(path, 'r') as file:
                df['content'][ind] = file.read()

    # make a list of urls after the most recent url that was found
    last_found = df[df['content'] != ''].index[-1]

    # # make a list of urls that are not saved locally and should be fetched
    lookup_urls = [
        (ind, url) for ind, url, content
        in zip(df.index[last_found:], df['url'][last_found:], df['content'][last_found:])
        if not content and df['date'][ind]
    ]

    # fetch url contents and add to df
    if len(lookup_urls) > 0:

        # create browser
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        browser = loop.run_until_complete(_open_browser())

        # add content to the dataframe
        lookup_idx = [i[0] for i in lookup_urls]  # save the indices for adding to df
        lookup_urls = [i[1] for i in lookup_urls]  # urls to fetch from

        # batch size. we rate limit to one batch per minute
        n = 15

        # iterate over batches
        for i in range(0, len(lookup_idx), n):

            print('starting loop')

            # record time at start
            t0 = time.time()

            # get content
            df['content'][lookup_idx[i:i+n]] = loop.run_until_complete(_gather_results(lookup_urls[i:i+n]))

            # time at end
            t1 = time.time()

            print('finished loop')

            # wait if a minute hasn't passed yet
            if t1 < t0 + 60:
                time.sleep(t0 + 60 - t1)

    return df

def download_stuff(
        num_domains=12000,
        start_date: datetime = datetime(2000, 1, 1),
        end_date=datetime(2022, 1, 1)
) -> None:

    df = get_domains(num_domains=num_domains)
    print('got domains')

    df = get_dates(df=df, start_date=start_date, end_date=end_date)
    print('got dates')

    df = get_content(df=df)
    print('got content')


if __name__ == "__main__":

    download_stuff(num_domains=1000)

