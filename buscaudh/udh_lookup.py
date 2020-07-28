import re
import zeep
from statistics import mean
from geopy.geocoders import Nominatim, ArcGIS
from geopy.exc import GeocoderUnavailable
from shapely.geometry import Point
import pandas as pd
import geopandas as gpd
import importlib_resources
from buscaudh.exceptions import CEPException,\
    GeolocationException
import requests
from collections import OrderedDict

cep_match = r'(\d{2})\.?(\d{3})-?([\d]{3})'
_correios_wsdl = 'https://apps.correios.com.br/SigepMasterJPA/'\
    'AtendeClienteService/AtendeCliente?wsdl'

_data_root = importlib_resources.files('buscaudh.data')
_udh_gdf = gpd.read_file(_data_root/'RM_Natal_UDH_2_region.shp')

# format the CEP
def fix_cep(cep):
    cep_fmt = re.search(cep_match, cep)
    if not cep_fmt:
        raise CEPException("CEP inválido:"
            " {}".format(cep))
    return "{}{}-{}".format(*cep_fmt.group(1,2,3))
        
# This function will return the address associated with the
# CEP code. If CEP is not in cache, will try to solve the
# address from CORREIOS webservice.
def cep_to_addr(cep, retry=0, max_retries=1, addr_cache={}, use_correios=True):
    cep = fix_cep(cep)

    if addr_cache:
        addr = addr_cache.get(cep)
        if addr:
            address = {k:v for k,v in addr.items()}
            address['cep'] = cep
            return address
    # else: not in cache, will try to get info from correios

    # If not to use correios webservice and not in cache
    if not use_correios:
        raise ValueError("CEP not in address cache.")

    # connect to webservice
    try:
        client = zeep.Client(wsdl=_correios_wsdl)
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout) as ex:
        if retry < max_retries:
            return cep_to_addr(cep, retry+1, addr_cache=addr_cache)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        if addr_cache:
            addr_cache[cep] = {}
        raise CEPException(def_msg="not found") from ex

    # query
    try:
        info = client.service.consultaCEP(cep)
    except (requests.exceptions.ConnectionError,
        zeep.exceptions.TransportError) as ex:
        if retry < max_retries:
            return cep_to_addr(cep, retry+1, addr_cache=addr_cache)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        if addr_cache:
            addr_cache[cep] = {}
        raise CEPException(def_msg="not found") from ex
    if not info:
        if addr_cache:
            addr_cache[cep] = {}
        raise CEPException(def_msg="not found")

    comp = ""
    if info["complemento2"]:
        # add house number if available
        numbers = [int(n) for n in re.findall(r'\d+',
            info["complemento2"])]
        if len(numbers) > 1:
            # taking mean if number interval
            number = int(mean(numbers))
            # and adjusting parity
            if numbers[0] % 2 != number % 2:
                number += 1
            comp = "{}".format(number)
        elif numbers:
            comp = "{}".format(numbers[0])

    return_object = {
        "logradouro": info["end"],
        "complemento": comp,
        "bairro": info["bairro"],
        "cidade": info["cidade"],
        "estado": info["uf"],
        "cep": cep,
        "longitude": '',
        "latitude": ''
    }

    # update the cache if it exists
    if addr_cache:
        addr_cache[cep] = {k:v for k,v in\
            return_object.items() if k != 'cep'}

    return return_object

def _build_addr(info):
    addr_str = ""
    end = info["logradouro"]
    if pd.notnull(end):
        if pd.notnull(info["complemento"]):
            end += " {}".format(info["complemento"])
        addr_str += "{}, ".format(end)
    if pd.notnull(info["bairro"]):
        addr_str += "{}, ".format(info["bairro"])
    if not (pd.notnull(info["cidade"]) and
            pd.notnull(info["estado"])):
        raise ValueError("Endereço com cidade "\
            "e/ou estado inválido: {}".format(info))
    addr_str += "{}, {}, Brazil".format(
        info["cidade"], info["estado"])
    return addr_str


# Geolocate an address
def addr_to_geo(address, gc_cod="osm", geoloc_caches={}):
    search_addr = _build_addr(address)
    
    if gc_cod == "osm":
        gc = Nominatim(user_agent="buscaudh")
    elif gc_cod == "arcgis":
        gc = ArcGIS()
    else:
        raise ValueError("Invalid gc_cod {}".format(gc_cod))

    try:
        res = gc.geocode(search_addr)
    except GeocoderUnavailable as exc:
        raise GeolocationException("Serviço de "
            "geolocalização indisponível") from exc

    if not res:
        # Address not found
        cache = geoloc_caches.get(gc_cod)
        if cache:
            cache[address['cep']] = {}
        if gc_cod == "osm":
            return addr_to_geo(address, "arcgis", geoloc_caches)
        raise GeolocationException("Geolocalização de "
            "endereço não encontrado ({})".format(search_addr))

    cache = geoloc_caches.get(gc_cod)
    if cache:
        cache[address['cep']] = {"longitude":res.longitude,
            "latitude":res.latitude}

    address["longitude"] = res.longitude
    address["latitude"] = res.latitude

    return address

# geolocate a CEP
def cep_to_geo(cep, caches={}):
    try:
        cep = fix_cep(cep)
    except CEPException:
        return {}

    try:
        addr = cep_to_addr(cep, addr_cache=caches.get('addr_cache'))
        if pd.isnull(addr.get("cidade")):
            # if CEP with no associated address in cache, return
            return addr
        # else, address obtained, proceed with geolocation
    except CEPException:
        # address not found or multiple connection errors
        return {'cep': cep}

    # check geolocation caches
    cep = addr['cep']
    geoloc = None
    if 'geoloc_caches' in caches:
        for gc_cod, gc in caches.get('geoloc_caches').items():
            check = gc.get(cep)
            if check:
                # if cep in cache, save result in geoloc
                geoloc = check
                if geoloc.get("latitude") and geoloc.get("longitude"):
                    # if successful geoloc in cache, return
                    addr["latitude"] = geoloc["latitude"]
                    addr["longitude"] = geoloc["longitude"]
                    return addr
        if geoloc:
            # if unsuccessful geoloc in any cache, return
            return addr

    # else, try geolocation
    try:
        geoloc = addr_to_geo(addr,
            geoloc_caches=caches.get('geoloc_caches'))
    except GeolocationException:
        return addr
    return geoloc


# Main function to look for UDH code based on CEP code
def cep_to_udh(cep, caches={}):
    if pd.isnull(cep):
        return {}

    # get the geolocation of the CEP code
    geoloc = cep_to_geo(cep, caches)

    # if no location available, return None 
    if not geoloc.get("latitude") or not geoloc.get("longitude"):
        geoloc["udh"] = None
        return geoloc

    # if location available, will check if within limits of one UDH

    if caches.get('udh_cache'):
        geoloc['udh'] = caches['udh_cache'].get(geoloc['cep'])
        if geoloc['udh']:
            return geoloc

    p = Point(geoloc["longitude"], geoloc["latitude"])
    udh = None
    for shape in _udh_gdf.itertuples(index=False):
        if p.within(shape.geometry):
            udh = shape.UDH_ATLAS
            break
    else:
        # not within any UDH
        geoloc["udh"] = None
        return geoloc
    geoloc["udh"] = udh

    return geoloc

def ceps_to_udhs(ceps):
    # load cache
    caches = {
        "addr_cache": pd.read_csv(_data_root / 'all_cep_to_addr.csv',
            dtype={"complemento":str}, index_col="cep").to_dict('index'),
        "geoloc_caches": OrderedDict([(gc, pd.read_csv(_data_root/
            'all_{}_locations.csv'.format(gc),
            index_col="cep").to_dict('index'))for gc in
            ("custom", "osm", "arcgis")]),
        "udh_cache": pd.read_csv(_data_root / 'cep_to_udh.csv',
            dtype={"udh": str}, index_col="cep").to_dict('index')
    }
    
    for cep in ceps:
        yield cep_to_udh(cep, caches)

    df = pd.DataFrame.from_dict(caches['addr_cache'], 'index')
    df.index.name = 'cep'
    df.to_csv(_data_root / 'all_cep_to_addr.csv')

    for gc_code, cache in caches["geoloc_caches"].items():
        df = pd.DataFrame.from_dict(cache, 'index')
        df.index.name = 'cep'
        df.to_csv(_data_root/'all_{}_locations.csv'.format(gc_code))