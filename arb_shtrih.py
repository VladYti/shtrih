import os
import csv
import datetime
from argparse import ArgumentParser

from tqdm import tqdm
import pandas as pd
import oracledb
from dbfpy3 import dbf


def log(ct: int, name: str) -> None:
    """
    Service function, create file log.txt
    :param ct: number of rows was writen
    :param name: filename
    :return: none
    """
    with open('log.txt', 'a', encoding='UTF-8') as logfile:
        logfile.write(f'{datetime.datetime.now()} -- {ct} total row write to {name} \n')


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


def convert7(rn7: str) -> str:
    """
    DENIED
    :param rn7:
    :return:
    """
    return ''.join(str(ord(i)).zfill(3) for i in list(rn7))


def dataframe_from_csv(csv_name: str, tp: str = 'base', version: int = 7) -> pd.DataFrame:
    """
    Create pd.DataFrame object from passed filename parameter
    :param csv_name: csv filename
    :param tp: type of passing csv file (invsoot, sinbase, etc.)
    :param version: db version
    :return: pd.DataFrame object
    """
    dtype_imp = {'RN7': 'str', 'RN8': pd.Int64Dtype(), 'SRN7': pd.Int64Dtype()}
    dtype_soot = {'IRN': pd.Int64Dtype(),
                  'KRN': pd.Int64Dtype(),
                  'SRN': pd.Int64Dtype(),
                  'CODE': 'str'}

    dtype_base7 = {'RN': pd.Int64Dtype(), 'NRN': pd.Int64Dtype(), 'ARN': pd.Int64Dtype(), 'MRN': pd.Int64Dtype(),
                   'KRN': pd.Int64Dtype(), 'GRP': pd.Int64Dtype(),
                   'NUM': 'str', 'KART': 'str', 'PASS': 'str', 'ZAV': 'str', 'TIP': 'float', 'SUM': 'float',
                   'DAT': 'str', 'KOL': 'float', 'HND': 'bool', 'PR1': 'str', 'PRM': 'str', 'OKOF': 'str'}

    dtype_base8 = {'RN': 'int64', 'NRN': 'int64', 'ARN': 'int64', 'MRN': 'int64', 'ZRN': 'str',
                   'KRN': 'str', 'GRP': 'str',
                   'NUM': 'str', 'KART': 'str', 'PASS': 'str', 'ZAV': 'str', 'HAR': 'str',
                   'MODEL': 'str', 'TIP': 'float', 'SUM': 'float', 'DAT': 'str', 'KOL': 'float',
                   'HND': 'bool', 'PR1': 'str', 'PRM': 'str', 'OKOF': 'str'}
    if tp == 'base':
        if version == 7:
            db = pd.read_csv(csv_name, delimiter='@', dtype=dtype_base7)
        else:
            db = pd.read_csv(csv_name, delimiter='@')
    elif tp == 'soot':
        db = pd.read_csv(csv_name, delimiter='@', dtype=dtype_soot, encoding="ISO-8859-1")
    else:
        db = pd.read_csv(csv_name, delimiter='@', dtype=dtype_imp, encoding='ISO-8859-1')
    return db


def oracle_conn(instant_cli: str, host: str, service: str, authid: str, password: str) -> None:
    """
    Function that connect to oracle db, collect data from table 'IMPORT7', and write it to csv file
    :param instant_cli: path to oracle_instantclient folder
    :param host: oracle db host (IP)
    :param service: oracle service name (oracle_sid)
    :param authid: (username)
    :param password: (user password)
    :return: None
    """
    oracledb.init_oracle_client(lib_dir=instant_cli)
    connection = oracledb.connect(user=authid, password=password, host=host, port=1521, service_name=service)

    with open('import7.csv', 'w', newline='', encoding='UTF-8') as txt:
        cursor = connection.cursor()
        writer = csv.writer(txt, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['RN7', 'RN8', 'SRN7'])
        inventory = cursor.execute(
            'select i.rn7, i.rn8, LPAD(ASCII(SUBSTR(I.RN7, 1, 1)), 3, \'0\') || LPAD(ASCII(SUBSTR(I.RN7, 2, 1)), 3, '
            '\'0\') || LPAD(ASCII(SUBSTR(I.RN7, 3, 1)), 3, \'0\') || LPAD(ASCII(SUBSTR(I.RN7, 4, 1)), 3, \'0\') from '
            'import7 i where i.table7=\'INBASE\'')
        for row in tqdm(inventory):
            rn7, rn8, rn_shtrih = row
            # rn_shtrih = ''.join(str(ord(i)).zfill(3) for i in [s for s in rn7])
            writer.writerow([rn7, rn8, rn_shtrih[1:]])
            # print(rn8[0], rn7[0], rn_shtrih)
        cursor.close()

    with open('udo_import7.csv', 'w', newline='', encoding='UTF-8') as txt:
        cursor = connection.cursor()
        writer = csv.writer(txt, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['SRN7', 'RN7', 'RN8'])
        tmp = cursor.execute(
            'select ui.rn7, LPAD(ASCII(SUBSTR(uI.RN7, 1, 1)), 3, \'0\') || LPAD(ASCII(SUBSTR(uI.RN7, 2, 1)), 3, '
            '\'0\') || LPAD(ASCII(SUBSTR(uI.RN7, 3, 1)), 3, \'0\') || LPAD(ASCII(SUBSTR(uI.RN7, 4, 1)), 3, \'0\'), '
            'ui.rn8 from udo_import7 ui where trim(ui.table7)=\'INSOST\''
        )
        for row in tqdm(tmp):
            writer.writerow(row)
        cursor.close()
    connection.close()




def read_dbf(path7: str, path8: str) -> None:
    """
    Function read input dbf's files, version 7 and 8, and write it to csv files
    :param path7: path to 7's dbf's folder
    :param path8: path to 8's dbf's folder
    :return: none
    """
    count = 0
    path7_inv = path7 + '\\InvSoot.dbf'
    path8_inv = path8 + '\\InvSoot.dbf'

    db7 = dbf.Dbf(path7_inv)
    db8 = dbf.Dbf(path8_inv)

    with open('InvSoot7.csv', 'w', newline='', encoding='utf-8') as csv7:
        spamwriter = csv.writer(csv7, delimiter='@',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([i.decode('UTF-8') for i in db7.field_names])
        for row in tqdm(db7, desc='Import from dbf to CSV file'):
            spamwriter.writerow([str(i).strip() for i in row])
            count += 1
    db7.close()
    log(count, 'InvSoot7.dbf')

    count = 0
    with open('InvSoot8.csv', 'w', newline='', encoding='utf-8') as csv8:
        spamwriter = csv.writer(csv8, delimiter='@',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([i.decode('UTF-8') for i in db8.field_names])
        for row in tqdm(db8):
            count += 1
            spamwriter.writerow([str(i).strip() for i in row])
    db8.close()
    log(count, 'InvSoot8.dbf')

    path7_base = path7 + '\\SINBASE.dbf'
    path8_base = path8 + '\\SINBASE.dbf'

    db7_base = dbf.Dbf(path7_base)
    db8_base = dbf.Dbf(path8_base)



    count = 0
    with open('SINBASE7.csv', 'w', newline='', encoding='utf-8') as csv7_base:
        writer = csv.writer(csv7_base, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([i.decode('UTF-8') for i in db7_base.field_names])
        for row in tqdm(db7_base):
            writer.writerow([str(i).strip() for i in row])
            count += 1
        db7_base.close()
        log(count, 'SInBase7.dbf')

    count = 0
    with open('SINBASE8.csv', 'w', newline='', encoding='utf-8') as csv8_base:
        writer = csv.writer(csv8_base, delimiter='@',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([i.decode('UTF-8') for i in db8_base.field_names])
        for row in tqdm(db8_base):
            writer.writerow([str(i).strip() for i in row])
            count += 1
        db8_base.close()
        log(count, 'SInBase8.dbf')


def log_proc(args):
    """
    service function, logging something, write it to log_proc.txt
    :param args: some arguments
    """
    with open('log_proc.txt', 'a', encoding='utf-8') as f:
        # f.write(f'{args[0]}, {args[1]}, {args[2]}\n')
        f.write(' -| '.join([str(i) for i in args]))
        f.write('\n')
        f.write('_' * 40 + '\n')


def process(db7: pd.DataFrame, db8: pd.DataFrame, db_imp: pd.DataFrame):
    """

    :param db7:
    :param db8:
    :param db_imp:
    """
    count = 0

    uimport = pd.read_csv('udo_import7.csv', delimiter='@')
    for irn8 in tqdm(db8.IRN, desc='Process database'):


        pass

    # vis = set()
    # for rn8 in tqdm(db8.IRN, desc='Process database'):
    #     if len(list(db_imp.loc[db_imp['RN8'] == rn8, 'SRN7'])) == 0:
    #         log_proc(['Not_found', rn8, '\n'])
    #         continue
    #     rn7 = list(db_imp.loc[db_imp['RN8'] == rn8, 'SRN7'])[0]
    #     code7 = list(db7.loc[db7['IRN'] == rn7, 'CODE'])
    #     if len(code7) > 1:
    #         krn7 = list(db7.loc[db7['IRN'] == rn7, 'KRN'])
    #         krn8 = list(db8.loc[db8['IRN'] == rn8, 'KRN'])
    #         log_proc(['Group_card \n', rn8, krn8, len(krn8), '\n', rn7, krn7, len(krn7), '\n', code7])
    #         if len(krn7) - len(krn8) > 1:
    #             if rn8 not in vis:
    #                 vis.add(rn8)
    #                 count += 1
    #             log_proc(['+' * 10, '\n'])
    #
    #     else:
    #         log_proc([rn8, rn7, code7])
    #
    # log_proc(['Bag card count - ', count])


def main(args) -> int:
    """
    Main function, call everything else
    :param args: input path to pars.txt
    :return: none
    """
    PARAMS_PATH = args.path
    params = read_params(PARAMS_PATH)

    read_dbf(params['dbf7'], params['dbf8'])
    db_sinbase8 = dataframe_from_csv('SINBASE8.csv', tp='base', version=8)
    db_sinbase7 = dataframe_from_csv('SINBASE7.csv', tp='base', version=7)

    print(db_sinbase7.info())
    db_invsoot7 = dataframe_from_csv('InvSoot7.csv', tp='soot')
    db_invsoot8 = dataframe_from_csv('InvSoot8.csv', tp='soot')

    oracle_conn(params['cli'], params['host'], params['service'], params['authid'], params['password'])
    db_import7 = dataframe_from_csv('import7.csv', tp='imp')
    print(db_import7.info(), 'import7')
    print(db_invsoot8.info(), 'invsoost8')
    print(db_invsoot7.info(), 'invsoost7')

    process(db_invsoot7, db_invsoot8, db_import7)

    return 0


if __name__ == '__main__':
    parser = ArgumentParser(prog='Shtrih import codes from 7 to 8',
                            description='This program was create for import \'codes\' from SHTRIH.DBF(7) to SHTRIH.DBF(8)',
                            epilog='Work spase required: Oracle_instantClient_11_2 (directory)\n Path to 7\'s and 8\' DBF\'s files')
    parser.add_argument('--path', type=dir_path, default='./pars.txt', required=False,
                        help='Path to *.txt file with all neaded parameters\n (See example.txt)')
    args = parser.parse_args()
    main(args)