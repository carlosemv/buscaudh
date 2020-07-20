#!/usr/bin/env python

from buscaudh.exceptions import CEPException
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
        cols = ("cep", "cidade", "uf", "bairro", "udh")

        print("Carregando dados...", end=" ", flush=True)
        if args.sesap_file.endswith(".xlsx"):
            df = pd.read_excel(args.sesap_file,
                sheet_name="BANCO", dtype={"CEP":str},
                usecols=["CEP"], engine='xlrd')
        else:
            df = pd.read_csv(args.sesap_file,
                sep=';', index_col=0)
        print("Dados de entrada carregados.")

        print("Processando...", flush=True)
        t0 = time()
        for cep in df.CEP:
            if pd.notnull(cep):
                try:
                    info = lookup_udh(cep)
                except CEPException:
                    output.append({k:None for k in cols})
                else:
                    output.append({k:info.get(k) for k in cols})
            else:
                output.append({k:None for k in cols})
        print("Completo em {}s".format(time()-t0))

        print("Escrevendo para arquivos de saída...",
            end=" ", flush=True)
        new_df = pd.DataFrame(output)
        new_df.to_csv(args.output+".csv")
        new_df.to_excel(args.output+".xlsx")
        print("Saída escrita.")
