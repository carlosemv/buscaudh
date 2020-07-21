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

cep_match = r'(\d{2})\.?(\d{3})-?([\d]{3})'
_correios_wsdl = 'https://apps.correios.com.br/SigepMasterJPA/'\
    'AtendeClienteService/AtendeCliente?wsdl'

_data_root = importlib_resources.files('buscaudh.data')
_udh_gdf = gpd.read_file(_data_root/'RM_Natal_UDH_2_region.shp')
_addr_cache = pd.read_csv(_data_root / 'all_addresses.csv',
    dtype={"complemento":str})
_geoloc_caches = tuple((gc, pd.read_csv(_data_root/'all_{}_locations'\
    '.csv'.format(gc))) for gc in ("custom", "osm", "arcgis"))

def cep_address(cep, retry=0, max_retries=1):
    try:
        client = zeep.Client(wsdl=_correios_wsdl)
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout):
        if retry < max_retries:
            return cep_address(cep, retry+1)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        raise CEPException(def_msg="not found") from ex

    try:
        info = client.service.consultaCEP(cep)
    except (requests.exceptions.ConnectionError,
        zeep.exceptions.TransportError) as ex:
        if retry < max_retries:
            return cep_address(cep, retry+1)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        raise CEPException(def_msg="not found") from ex
    if not info:
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

    return {
        "logradouro": info["end"],
        "complemento": comp,
        "bairro": info["bairro"],
        "cidade": info["cidade"],
        "estado": info["uf"],
        "cep": cep
    }

def _build_address(info):
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

def geolocate(address, gc_cod="osm"):
    search_addr = _build_address(address)
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
        if gc_cod == "osm":
            return geolocate(address, "arcgis")
        raise GeolocationException("Geolocalização de "
            "endereço não encontrado ({})".format(search_addr))

    address["longitude"] = res.longitude
    address["latitude"] = res.latitude
    return address

def geolocate_cep(cep):
    cep_fmt = re.search(cep_match, cep)
    if not cep_fmt:
        raise CEPException("CEP inválido:"
            " {}".format(cep))
    cep = "{}{}-{}".format(*cep_fmt.group(1,2,3))
    addr = _addr_cache.loc[_addr_cache.cep == cep]
    if addr.shape[0] > 1:
        raise ValueError("Endereço "
            "ambíguo em cache para cep {}".format(cep))
    elif addr.shape[0] == 1:
        addr = addr.iloc[0].to_dict()
        if pd.isnull(addr["cidade"]):
            # if CEP with no associated address in cache, return
            return addr
        # else, there is valid address, proceed with geolocation
    else:
        # CEP not in cache, try and get address
        try:
            addr = cep_address(cep)
        except CEPException:
            # TODO: add to cache
            return {"cep": cep}

    geoloc = None
    for gc_cod, gc in _geoloc_caches:
        df = gc.loc[gc.cep == cep]
        if df.shape[0] > 1:
            raise ValueError("Geolocalização ambígua"\
                "em cache ({}) para cep {}".format(gc_cod, cep))
        elif df.shape[0] == 0:
            continue

        geoloc = df.iloc[0].to_dict()
        if geoloc["latitude"] and geoloc["longitude"]:
            # if successful geoloc in cache, return
            addr["latitude"] = geoloc["latitude"]
            addr["longitude"] = geoloc["longitude"]
            return addr

    if geoloc:
        # if unsuccessful geoloc in cache, return
        return addr

    # else, try geolocation
    try:
        geoloc = geolocate(addr)
    except GeolocationException:
        return addr
    return geoloc

# Main function to look for UDH code based on CEP code
def lookup_udh(cep):
    geoloc = geolocate_cep(cep)
    if not geoloc.get("latitude") or not geoloc.get("longitude"):
        geoloc["udh"] = None
        return geoloc

    p = Point(geoloc["longitude"], geoloc["latitude"])
    udh = None
    for shape in _udh_gdf.itertuples(index=False):
        if p.within(shape.geometry):
            udh = shape.UDH_ATLAS
            break
    geoloc["udh"] = udh
    return geoloc
    

