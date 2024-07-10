import os
import csv
import datetime
from argparse import ArgumentParser

from tqdm import tqdm
import pandas as pd
import oracledb
from dbfpy3 import dbf as dbfpy
import dbf


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
    Optional feature (doesn't do much)
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

    with open('..\\.temp_files\\import7.csv', 'w', newline='', encoding='UTF-8') as txt:
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

    with open('..\\.temp_files\\udo_import7.csv', 'w', newline='', encoding='UTF-8') as txt:
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

    db7 = dbfpy.Dbf(path7_inv)
    db8 = dbfpy.Dbf(path8_inv)

    with open('..\\.temp_files\\InvSoot7.csv', 'w', newline='', encoding='utf-8') as csv7:
        spamwriter = csv.writer(csv7, delimiter='@',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([i.decode('UTF-8') for i in db7.field_names])
        for row in tqdm(db7, desc='Import from dbf to CSV file'):
            spamwriter.writerow([str(i).strip() for i in row])
            count += 1
    db7.close()
    log(count, 'InvSoot7.dbf')

    count = 0
    with open('..\\.temp_files\\InvSoot8.csv', 'w', newline='', encoding='utf-8') as csv8:
        spamwriter = csv.writer(csv8, delimiter='@',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        spamwriter.writerow([i.decode('UTF-8') for i in db8.field_names])
        for row in tqdm(db8):
            count += 1
            spamwriter.writerow([str(i).strip() for i in row])
    db8.close()
    log(count, 'InvSoot8.dbf')

    # Reading 'SINBASE.dbf'
    # Rejected section (not used)
    # path7_base = path7 + '\\SINBASE.dbf'
    # path8_base = path8 + '\\SINBASE.dbf'

    # db7_base = dbfpy.Dbf(path7_base)
    # db8_base = dbfpy.Dbf(path8_base)

    # count = 0
    # with open('SINBASE7.csv', 'w', newline='', encoding='utf-8') as csv7_base:
    #     writer = csv.writer(csv7_base, delimiter='@',
    #                         quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #     writer.writerow([i.decode('UTF-8') for i in db7_base.field_names])
    #     for row in tqdm(db7_base):
    #         writer.writerow([str(i).strip() for i in row])
    #         count += 1
    #     db7_base.close()
    #     log(count, 'SInBase7.dbf')
    #
    # count = 0
    # with open('SINBASE8.csv', 'w', newline='', encoding='utf-8') as csv8_base:
    #     writer = csv.writer(csv8_base, delimiter='@',
    #                         quotechar='|', quoting=csv.QUOTE_MINIMAL)
    #     writer.writerow([i.decode('UTF-8') for i in db8_base.field_names])
    #     for row in tqdm(db8_base):
    #         writer.writerow([str(i).strip() for i in row])
    #         count += 1
    #     db8_base.close()
    #     log(count, 'SInBase8.dbf')


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


def process(db7: pd.DataFrame,
            db8: pd.DataFrame,
            db_imp: pd.DataFrame,
            invsubst: pd.DataFrame) -> pd.DataFrame:
    """

    :param invsubst:
    :param db7:
    :param db8:
    :param db_imp:
    """

    codes = set()
    codes_list = dict()

    max_code = 14641
    i = 0
    for rn8 in tqdm(db8.iterrows(), desc='Process database'):
        i += 1
        idx = list(rn8)[0]

        if pd.isna(rn8[1].KRN):
            rn7 = list(db_imp.loc[db_imp['RN8'] == rn8[1].IRN]['SRN7'])
            if len(rn7) == 0:
                log_proc(['(KRN is NAN) and (import7 not contains rn7)', 'error', rn8[1].IRN, '\n'])

            else:
                rn7 = rn7[0]
                code = list(db7.loc[(db7['IRN'] == rn7) & (db7['KRN'].isnull())]['CODE'])
                if len(code) == 0:
                    tmp = db7.loc[(db7['IRN'] == rn7)]
                    if len(list(tmp['CODE'])) == 0:
                        codes_list[idx] = '000' + str(max_code + i)
                        log_proc(['(MAX ERROR) :(KRN is NAN) and (invsoot7 not contains rn7)', rn8[1].IRN, '\n'])
                    else:
                        codes_list[idx] = '000' + str(max_code + i)
                        log_proc(
                            ['(KRN is NAN) and (KRN is not NAN in invsoot7)', 'error', rn8[1].IRN, '\n', tmp, '\n'])
                else:
                    if code[0] in codes:
                        log_proc(['err'])
                    else:
                        codes.add(code[0])
                        codes_list[idx] = code[0]
                        log_proc(['ALL RIGHT', rn8[1].IRN, ' --> ', rn7, ': ', code[0], '\n'])
            pass

        else:
            rn7 = list(db_imp.loc[db_imp['RN8'] == rn8[1].IRN]['SRN7'])
            if len(rn7) == 0:
                log_proc(['(KRN is not NAN) and (import7 not contains rn7)', 'error', rn8[1].IRN, '\n'])
                continue
            else:
                rn7 = rn7[0]

            invs_rn7 = list(invsubst.loc[invsubst['RN8'] == rn8[1].KRN]['RN7'])
            if len(invs_rn7) == 0:
                codes_list[idx] = '000' + str(max_code + i)
                log_proc(['InvSUBST nomen is undefined', 'error', rn8[1].IRN, rn8[1].KRN, 'Неизвестно', '\n'])
                continue
            else:
                invs_rn7 = invs_rn7[0]

            code7 = list(db7.loc[(db7['IRN'] == rn7) & (db7['KRN'] == invs_rn7)]['CODE'])[0]
            if code7 in codes:
                log_proc(['error'])
            else:
                codes.add(code7)
                codes_list[idx] = code7
                log_proc(['ALL RIGHT', rn8[1].IRN, rn8[1].KRN, ' --> ', rn7, invs_rn7, ' --> ', code7, '\n'])

        pass

    for key, value in tqdm(codes_list.items(), desc='Changing codes'):
        db8.loc[key, 'CODE'] = value

    return db8


def main(args) -> int:
    """
    Main function, call everything else
    :param args: input path to pars.txt
    :return: none
    """
    # Reading input parameters
    PARAMS_PATH = args.path
    params = read_params(PARAMS_PATH)

    # Reading data from dbf files
    # More specific: files 'InvSoot.dbf' and 'SinBase.dbf'
    read_dbf(params['dbf7'], params['dbf8'])

    # Create pd.DataFrame objects from read dbf's
    db_invsoot7 = dataframe_from_csv('InvSoot7.csv', tp='soot')
    db_invsoot8 = dataframe_from_csv('InvSoot8.csv', tp='soot')

    # Reading table import7 and udo_import7 (contains table7 'INSOST') from oracle database
    oracle_conn(params['cli'], params['host'], params['service'], params['authid'], params['password'])

    # Convert those tables to pd.DataFrame objects
    db_import7 = dataframe_from_csv('import7.csv', tp='imp')
    db_invsubst = pd.read_csv('udo_import7.csv', delimiter='@', dtype={'RN7': pd.Int64Dtype(), 'RN8': pd.Int64Dtype()})

    # Main process function
    # Replace codes from main 'InvSoot.dbf' file with codes from 'InvSoot.dbf' by Parus-7
    result_db = process(db_invsoot7, db_invsoot8, db_import7, db_invsubst)

    # Write data to existing 'InvSoot.dbf' file new codes
    db8 = dbf.Table(params['dbf8'] + '\\InvSoot.dbf')
    print(db8.field_names)
    db8.open(mode=dbf.READ_WRITE)
    for row in db8:
        irn = int(row.IRN)
        krn = None if str(row.KRN).strip() == '' else int(row.KRN)
        if pd.isna(krn):
            new_code = list(result_db.loc[(result_db['IRN'] == irn) & result_db['KRN'].isnull()]['CODE'])
        else:
            new_code = list(
                result_db.loc[(result_db['IRN'] == irn) & (result_db['KRN'] == krn)][
                    'CODE'])
        with row:
            row.CODE = new_code[0] + ' '*12
    db8.close()

    return 0


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
