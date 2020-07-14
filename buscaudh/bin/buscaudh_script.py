#!/usr/bin/env python

from buscaudh import lookup_udh, cep_match
import re
import argparse

def cep_type(arg_value):
    cep = re.search(cep_match, arg_value)
    if not cep:
        raise argparse.ArgumentTypeError(
            "CEP inválido.")
    return "{}-{}".format(*cep.group(1,2))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--cep', '-c', required=True,
        type=cep_type, help='CEP para o qual buscar UDH.')
    args = parser.parse_args()

    print(lookup_udh(args.cep))
