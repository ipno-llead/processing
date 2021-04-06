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
        "https://www.dropbox.com/sh/ymvc81kjfqqv9m6/AADoEpTWUNef0AMH5GaZxMPea/New%20Orleans_CSD_PPRR_2009_realigned.csv?dl=1",  # noqa
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
    "louisiana_state_csc": [
        "https://www.dropbox.com/sh/9zndy2dz3w60etq/AAAqcTxDk8GVj3uvQbUto3XBa/Log%20Records/LouisianaState_CSC_LPRR_1991-2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/s/h0ordt45fsaq4p5/colonel_full_names.csv?dl=1",  # noqa
    ],
    "brusly_pd": [
        "https://www.dropbox.com/sh/s3qchv84i0j5tsd/AAAx1lt_f2iqEERkEjK7wj2_a/Complaints%20Data/Brusly_PD_CPRR_2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/0t6dvicvhztbtya/AAACRW8-Smg0C7TdfMHtHUxua/Brusly_PD_PPRR_2020.csv?dl=1"  # noqa
    ],
    "post_council": [
        "https://www.dropbox.com/sh/9fafm0yxbk4z8zt/AAC22Rm_L4FVRzuIZ-Rc_UL4a/POST_PPRR_11-6-2020.csv?dl=1"  # noqa
    ],
    "port_allen_pd": [
        "https://www.dropbox.com/sh/8zs58ak2rpah9iu/AABUS6JdCHYyvjXWWk--RtGLa/Port_Allen_CPRR_2017-2018_byhand.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/8zs58ak2rpah9iu/AAC6iWLPwTHpXGr6NvEBNzgQa/Port_Allen_CPRR_2019.csv?dl=1"  # noqa
    ],
    "port_allen_csd": [
        "https://www.dropbox.com/sh/5znysznypatmig0/AAD4gXEXXJMxnxNcK-TocbPDa/Port_Allen_CSD_PPRR_2020.csv?dl=1"  # noqa
    ],
    "madisonville_pd": [
        "https://www.dropbox.com/sh/bv473hrxugw65qn/AAC-288ely1bv-Wl_o_kF2q2a/Madisonville_PD_CPRR_2010-2020.csv?dl=1"  # noqa
    ],
    "madisonville_csd": [
        "https://www.dropbox.com/s/ovbrufp86phrc75/Madisonville_CSD_PPRR_2019.csv?dl=1"  # noqa
    ],
    "baton_rouge_da": [
        "https://www.dropbox.com/sh/vdpyhf3lbi7lxy9/AAB_Qc3S4usW8QMatRXimi_za/Baton_Rouge_DA_CPRR_2021.csv?dl=1"  # noqa
    ],
    "ipm": [
        "https://www.dropbox.com/sh/1zdvf08kjarefyf/AACXnVrPKERs0F5Hs4c9_3GUa/new_orleans_IAPRO_PPRR_1946-2018.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/1zdvf08kjarefyf/AAD1_AaJAU5_6Vk8nMMspO0_a/new_orleans_PD_CPRR_actions_taken_1931-2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/1zdvf08kjarefyf/AAB-HhVpDXo0sUazVJvNd7soa/new_orleans_PD_CPRR_allegations_1931-2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/1zdvf08kjarefyf/AADcG1KGz9dJ-OovJ2GrFt_za/new_orleans_PD_PPRR_1946-2018.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/1zdvf08kjarefyf/AACJmVSIiBlUL5j8cFa8-RuJa/new_orleans_PD_use_of_force_2012-2019.csv?dl=1",  # noqa
    ],
    "new_orleans_so": [
        "https://www.dropbox.com/sh/dsrzc28teybtf4q/AAAq2PQtK_cM5Mt3PpknAoMOa/new_orleans_so_cprr_2019_tabula.csv?dl=1"  # noqa
    ],
    "greenwood_pd": [
        "https://www.dropbox.com/sh/9yu4fe2z6xuwnaf/AAALIGMiq95b2ZylzxFcUdqga/greenwood_pd_cprr_2015-2020_byhand.csv?dl=1"  # noqa
    ],
    "st_tammany_so": [
        "https://www.dropbox.com/sh/dtb2hn54ma59q2h/AAB_DLd50KBMkYBODy1PP1sFa/St.%20Tammany_SO_PPRR_2020.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/mwabzn85manxzo6/AACR-N1crrGx43ZJ2CYdtqUSa/St.Tammany_SO_CPRR_Lists_2015-2019_tabula.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/mwabzn85manxzo6/AACgbqjIwVL3XetaFcQHo-e5a/st_tammany_department_codes_tabula.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/lx21aphi3lxw09y/AAB1AX8m9X4jFCITWgXqupSoa/st_tammany_so_cprr_2011-2020_tabula.csv?dl=1",  # noqa
        "https://www.dropbox.com/sh/lx21aphi3lxw09y/AADn_CNJdKQfYUVaMR116l8aa/st_tammany_so_cprr_2020-2021_tabula.csv?dl=1",  # noqa
    ],
    "plaquemines_so": [
        "https://www.dropbox.com/s/g1b98kn4e4k1dtr/plaquemines_so_cprr_2019.csv?dl=1",  # noqa
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
            if resp.status_code >= 400:
                raise Exception("Error getting %s, status code = %d" %
                                (filename, resp.status_code))
            with open(full_filename, "wb") as f:
                f.write(resp.content)
            print("downloaded data/%s/%s" % (folder, filename))


if __name__ == "__main__":
    download_data_files()
