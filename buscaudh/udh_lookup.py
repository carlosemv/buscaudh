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

udh_shapefile = importlib_resources.files(
    'buscaudh.data') /'RM_Natal_UDH_2_region.shp'
cep_match = r'(\d{2})\.?(\d{3})-?([\d]{3})'
_correios_wsdl = 'https://apps.correios.com.br/SigepMasterJPA/'\
    'AtendeClienteService/AtendeCliente?wsdl'
_geoloc_order = ("custom", "osm", "arcgis")

def cep_address(cep, retry=0, max_retries=1):
    try:
        client = zeep.Client(wsdl=_correios_wsdl)
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError):
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
    addr = ""
    if info["end"]:
        addr += "{}, ".format(info["end"]+comp)
    if info["bairro"]:
        addr += "{}, ".format(info["bairro"])
    addr += "{}, {}, Brazil".format(
        info["cidade"], info["uf"])
    return addr

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

    geoloc = None
    for gc in _geoloc_order:
        df_file = importlib_resources.files(
            'buscaudh.data') / 'all_{}_locations.csv'.format(gc)
        df = pd.read_csv(df_file)
        df = df.loc[df.cep == cep]
        if df.shape[0] > 1:
            raise GeolocationException("Geolocalização "
                "ambígua em cache ({})".format(gc))
        elif df.shape[0] == 0:
            continue

        geoloc = df.iloc[0].to_dict()
        if geoloc["latitude"] and geoloc["longitude"]:
            return geoloc

    if geoloc:
        return geoloc
    try:
        addr = cep_address(cep)
        geoloc = geolocate(addr)
    except CEPException:
        return {"cep": cep}
    except GeolocationException:
        return addr
    return geoloc

def lookup_udh(cep):
    geoloc = geolocate_cep(cep)
    if not geoloc.get("latitude") or not geoloc.get("longitude"):
        return geoloc

    p = Point(geoloc["longitude"], geoloc["latitude"])
    gdf = gpd.read_file(udh_shapefile)
    udh = None
    for shape in gdf.itertuples(index=False):
        if p.within(shape.geometry):
            udh = shape.UDH_ATLAS
            break
    geoloc["udh"] = udh
    return geoloc