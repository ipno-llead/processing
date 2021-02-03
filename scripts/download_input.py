#!/usr/bin/env python
import requests
import pathlib
import re
import os
from urllib.parse import urlparse, unquote

DATA_FILES = {
    "new_orleans_harbor_pd": [
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADPg86iDeZz6kEmBKc6Pv6Za/Data%20Collected/New%20Orleans%20Harbor%20PD/Administrative%20Data/New%20Orleans_Harbor%20PD_PPRR_2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAD6dx5Kj-bY1XRFJ5sWeHl0a/Data%20Collected/New%20Orleans%20Harbor%20PD/Complaints%20Data/Datasets/New%20Orleans_Harbor%20PD_CPRR_2014-2020.csv?dl=1"  # noqa
    ],
    "new_orleans_pd": [
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABVClAFyA1h-LtmYOCGY7-la/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/Department%20ID%20Description%20List.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAC6eT1qKY9DfgfVj781NMJXa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/Job%20Code%20Description%20List.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AACUNWDHkNmh8_TLg3mggr0Ra/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_1999.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABMgxyYgGH1Zuben338rtpya/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2000.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAAjMJihsXLmUb_KFnaIKn6Na/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2001.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAAiJKmE3SrX9iY10-nCZ2lRa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2002.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAClcL9IevAl1ZG3KoFQg5S5a/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2003.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADsUoAX0ERfIPfFseJUpvuTa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2004.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABZs6MA3vWh9UR-j7sbS1jAa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2005.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAAXIlYISJJllR4vvKCgdshga/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2006.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABKmXcbQIA1UM0wTXFEWK5Ka/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2007.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADCcL_5PPXAo84Lv9zCkXT_a/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2008.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABtXjpwMpLy5TKFD7-co07ba/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2009.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABridhmPH6rbyH4tHA2xTQha/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2010.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAC4akfoKn57ckuMiLeJ1kewa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2011.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAAyzOIATVj3CLZ5K2qqHJPAa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2012.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADT9yIa8NFxTK5Tp54H6GdIa/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2013.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAA-QJxnQYKjBFKIEAlpVKGta/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2014.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADkdyybix59ae9VZZrWjO6ka/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2015.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADvPZX_gZ_pCvMeTe9GDC3Ya/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2016.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AADch5Cbc00xyaKat_OHl3A_a/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2017.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAAdK7KHdebOrtXSxhiLi64ha/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2018.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AAD-Hi1Ae45yLjdPx9XFT3p_a/Data%20Collected/New%20Orleans%20PD/Administrative%20Data/New%20Orleans_PD_PPRR_2019.csv?dl=1"  # noqa
    ],
    "new_orleans_csd": [
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABGwHMKbzSeu0Yd4gspSXg2a/Data%20Collected/New%20Orleans%20Civil%20Service%20Department/Administrative%20Data/New%20Orleans_CSD_PPRR_2009.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/nv366qnl1vmhgny/AABVFv9T2bwTOI0XuHULfbD-a/Data%20Collected/New%20Orleans%20Civil%20Service%20Department/Administrative%20Data/New%20Orleans_CSD_PPRR_2014.csv?dl=1"  # noqa
    ],
    "baton_rouge_pd": [
        "https://www.dropbox.com/sh/rc4a4961qqxu0co/AADmtLqCxY_k5ZlbPKtOmcKHa/Data%20Collected/Baton%20Rouge%20Police%20Department/Baton%20Rouge_PD_CPRR_2018.csv?dl=1",  # noqa
    ],
    "baton_rouge_csd": [
        "https://www.dropbox.com/sh/rc4a4961qqxu0co/AAATpIeZNzv0NJlzfSFOJKqZa/Data%20Collected/Baton%20Rouge%20Civil%20Service%20Department/Baton%20Rouge_CSD_PPRR_2017.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/rc4a4961qqxu0co/AADEvr8QN4RVb7Gb_xSyT934a/Data%20Collected/Baton%20Rouge%20Civil%20Service%20Department/Baton%20Rouge_CSD_PPRR_2019.csv?dl=1",  # noqa
    ],
    "baton_rouge_fpcsb": [
        "https://www.dropbox.com/sh/qihmr1a6wiswr6f/AAAwyYt429HSaOO6ZlNeNQsca/Baton_Rouge_FPCSB_Logs_1992-2012.csv?dl=1"  # noqa
    ],
    "baton_rouge_so": [
        "https://www.dropbox.com/sh/7s3ernm7fea479k/AAAXbtXwRx59xb0zPe6ExzMFa/Baton_Rouge_SO_CPRR_2018.csv?dl=1"  # noqa
    ],
    "louisianna_state_csc": [
        "https://www.dropbox.com/sh/9zndy2dz3w60etq/AAAqcTxDk8GVj3uvQbUto3XBa/Log%20Records/LouisianaState_CSC_LPRR_1991-2020.csv?dl=1"  # noqa
    ],
    "brusly_pd": [
        "https://www.dropbox.com/sh/s3qchv84i0j5tsd/AAAx1lt_f2iqEERkEjK7wj2_a/Complaints%20Data/Brusly_PD_CPRR_2020.csv?dl=1"  # noqa
    ],
    "post_council": [
        "https://www.dropbox.com/sh/9fafm0yxbk4z8zt/AAC22Rm_L4FVRzuIZ-Rc_UL4a/POST_PPRR_11-6-2020.csv?dl=1"  # noqa
    ]
}
_current_dir = os.path.dirname(os.path.realpath(__file__))


def download_data_files():
    for folder, urls in DATA_FILES.items():
        data_dir = os.path.join(_current_dir, "../data", folder)
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
        for url in urls:
            o = urlparse(url)
            filename = unquote(o.path).split("/")[-1].lower()
            filename = re.sub(r'\s+', '_', filename)
            full_filename = os.path.join(data_dir, filename)
            if os.path.isfile(full_filename):
                continue
            resp = requests.get(url, allow_redirects=True)
            with open(full_filename, "wb") as f:
                f.write(resp.content)
            print("downloaded data/%s/%s" % (folder, filename))


if __name__ == "__main__":
    download_data_files()
