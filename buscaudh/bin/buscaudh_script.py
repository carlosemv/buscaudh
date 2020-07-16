#!/usr/bin/env python

from buscaudh import lookup_udh, cep_match
import re
import argparse
from time import time
import pandas as pd

def cep_type(arg_value):
    cep = re.search(cep_match, arg_value)
    if not cep:
        raise argparse.ArgumentTypeError(
            "CEP inválido.")
    return "{}{}-{}".format(*cep.group(1,2,3))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    input_grp = parser.add_mutually_exclusive_group(required=True)
    input_grp.add_argument('--cep', '-c',
        type=cep_type, help='CEP para o qual buscar UDH.')
    input_grp.add_argument('--sesap_file', '-s',
        help='Arquivo .csv contendo CEPs '\
        'para consulta pela SESAP.')
    parser.add_argument('--output', '-o', help='Nome para '\
        'arquivos de saída (sem extensão)')
    args = parser.parse_args()

    if args.sesap_file and not args.output:
        parser.error("Argumento --output "\
            "obrigatório para --sesap_file")

    if args.cep:
        print(lookup_udh(args.cep))
    elif args.sesap_file:
        output = []
        cols = ("cep", "bairro", "udh")

        df = pd.read_csv(args.sesap_file,
            sep=';', index_col=0)
        t0 = time()
        for row in df.itertuples(index=False):
            if pd.notnull(row.CEP):
                info = lookup_udh(row.CEP)
                output.append({k:info.get(k) for k in cols})
            else:
                output.append({k:None for k in cols})
        print("Completo em {}s".format(time()-t0))
        new_df = pd.DataFrame(output)
        new_df.to_csv(args.output+".csv")
        new_df.to_excel(args.output+".xlsx")
