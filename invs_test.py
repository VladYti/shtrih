import os
import csv
from argparse import ArgumentParser

import oracledb
import numpy as np
import pandas as pd
from tqdm import tqdm


def dir_path(string: str) -> str:
    """
    Service function, that checking if passed argument is correct
    :param string: input path parameter
    :return: input string, if it's correct
    """
    if os.path.isfile(string):
        return string
    raise NotADirectoryError(string)


def read_params(path: str) -> dict:
    """
    Service function, read parameters from pars.txt
    :param path: input path
    :return: dict with params
    """
    params = {}

    with open(path, 'r', encoding='UTF-8') as text:
        while True:
            line = text.readline()
            if 'ORACLE_CONNECTION' in line:
                params['cli'] = text.readline().split('-')[0].strip()
                params['host'] = text.readline().split('-')[0].strip()
                params['service'] = text.readline().split('-')[0].strip()
                params['authid'] = text.readline().split('-')[0].strip()
                params['password'] = text.readline().split('-')[0].strip()

            if 'DDBF_files' in line:
                params['dbf7'] = text.readline().split('-')[0].strip()
                params['dbf8'] = text.readline().split('-')[0].strip()

            if not line:
                break

    return params


def oracle_imp(instant_cli: str, host: str, service: str, authid: str, password: str) -> None:
    oracledb.init_oracle_client(lib_dir=instant_cli)
    connection = oracledb.connect(user=authid, password=password, host=host, port=1521, service_name=service)

    with open('..\\.temp_files\\inv_subst.csv', 'w', newline='', encoding='UTF-8') as txt:
        cursor = connection.cursor()
        writer = csv.writer(txt, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['RN', 'PRN', 'NOMEN'])
        invsubst = cursor.execute(
            'SELECT INVS.RN, INVS.PRN, INVS.NOMENCLATURE '
            'FROM INVSUBST INVS '
            'WHERE INVS.NOMENCLATURE != 17354601'
        )
        for row in tqdm(invsubst):
            writer.writerow(row)

    with open('..\\.temp_files\\insost.csv', 'w', newline='', encoding='UTF-8') as txt:
        cursor = connection.cursor()
        writer = csv.writer(txt, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['RN7', 'PRN', 'NOMEN'])
        insost = cursor.execute(
            'select pi2.rn, i1.rn8, i2.rn8 from P7_INSOST pi2 '
            'JOIN IMPORT7 i1 ON pi2.MASTER_RN = i1.RN7 '
            'JOIN IMPORT7 i2 ON pi2.RN_PRNOM = i2.RN7 '
            'WHERE i1.table7=\'INBASE\' AND i2.table7=\'NOBASE\' AND i2.rn8 != 17354601'
        )
        for row in tqdm(insost):
            writer.writerow(row)

    return None


def oracle123(instant_cli: str, host: str, service: str, authid: str, password: str) -> None:
    oracledb.init_oracle_client(lib_dir=instant_cli)
    connection = oracledb.connect(user=authid, password=password, host=host, port=1521, service_name=service)

    with open('..\\.temp_files\\invpack.csv', 'w', newline='', encoding='UTF-8') as txt:
        cursor = connection.cursor()
        writer = csv.writer(txt, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['PRN', 'RN'])
        invpack = cursor.execute(
            'SELECT ip.rn, inv.rn FROM invpack ip JOIN INVENTORY inv ON ip.prn=inv.rn '
        )
        for row in tqdm(invpack):
            writer.writerow(row)
        cursor.close()
    connection.close()
    return None


def dataframe_from_csv(csv_name: str) -> pd.DataFrame:
    db = pd.read_csv(csv_name, delimiter='@', encoding='UTF-8')
    return db


def oracle_insert(instant_cli: str, host: str, service: str, authid: str, password: str) -> None:
    invsubst = dataframe_from_csv('..\\.temp_files\\inv_subst.csv')
    insost = dataframe_from_csv('..\\.temp_files\\insost.csv')

    oracledb.init_oracle_client(lib_dir=instant_cli)
    connection = oracledb.connect(user=authid, password=password, host=host, port=1521, service_name=service)
    cursor = connection.cursor()

    cursor.execute(
        f'drop table {authid}.UDO_T_IMPORT7'
    )

    cursor.execute(
        'create table UDO_T_IMPORT7 '
        '(table7    varchar2(20),'
        ' RN7       varchar2(10),'
        ' RN8       NUMBER(17))'
    )

    for prn in tqdm(np.unique(invsubst.PRN, axis=0)):
        prn_nom = np.unique(invsubst.loc[invsubst['PRN'] == prn, 'NOMEN'])

        for nom in prn_nom:
            rn7_s = list(insost.loc[(insost['PRN'] == prn) & (insost['NOMEN'] == nom), 'RN7'])
            rn8_s = list(invsubst.loc[(invsubst['PRN'] == prn) & (invsubst['NOMEN'] == nom), 'RN'])

            # print(rn7_s, len(rn7_s))
            # print(rn8_s, len(rn8_s), '\n')
            for rn7, rn8 in zip(rn7_s, rn8_s):
                cursor.execute(
                    'insert into UDO_T_IMPORT7 '
                    '(table7, rn7, rn8) '
                    'values '
                    '( \' INSOST \' ,' + '\'' + rn7 + '\'' + ',' + str(rn8) + ')'
                )
                connection.commit()

    cursor.close()
    connection.close()


def main(args):
    PARAMS_PATH = args.path
    params = read_params(PARAMS_PATH)

    oracle_imp(params['cli'], params['host'],  params['service'], params['authid'], params['password'])
    oracle_insert(params['cli'], params['host'],  params['service'], params['authid'], params['password'])


if __name__ == '__main__':
    parser = ArgumentParser(prog='Shtrih import codes from 7 to 8',
                            description='This program was create for import \'codes\' from SHTRIH.DBF(7) to '
                                        'SHTRIH.DBF(8)',
                            epilog='Work spase required: Oracle_instantClient_11_2 (directory)\n Path to 7\'s and 8\' '
                                   'DBF\'s files')
    parser.add_argument('--path', type=dir_path, default='./pars.txt', required=False,
                        help='Path to *.txt file with all neaded parameters\n (See example.txt)')
    args = parser.parse_args()
    main(args)
