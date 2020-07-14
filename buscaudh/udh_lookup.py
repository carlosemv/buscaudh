import re
import zeep
from statistics import mean
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable
from shapely.geometry import Point
import geopandas as gpd
from importlib.resources import path
from buscaudh.exceptions import CEPException,\
    GeolocationException
import requests

with path('buscaudh.data',
        'RM_Natal_UDH_2_region.shp') as shp:
    udh_shapefile = shp
_correios_wsdl = 'https://apps.correios.com.br/SigepMasterJPA/'\
    'AtendeClienteService/AtendeCliente?wsdl'
cep_match = r'([0-9]{5})-?([0-9]{3})'

def cep_address(cep, retry=0):
    client = zeep.Client(wsdl=_correios_wsdl)
    try:
        info = client.service.consultaCEP(cep)
    except requests.exceptions.ConnectionError as ex:
        if retry == 0:
            return get_location(cep, 1)
        raise CEPException("Falha de conexão"
            "na consulta de CEP.") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        raise CEPException("Falha na consulta de CEP:"
            " CEP não encontrado.") from ex

    info = client.service.consultaCEP(cep)
    end = info["end"]
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
            end += " {}".format(number)
        elif numbers:
            end += " {}".format(numbers[0])
    return "{}, {}, {}, {}, Brazil".format(end,
        info["bairro"], info["cidade"], info["uf"])

def geolocate(address):
    gc = Nominatim(user_agent="buscaudh")
    try:
        res = gc.geocode(address)
    except GeocoderUnavailable as exc:
        raise GeolocationException("Serviço de "
            "geolocalização indisponível") from exc
    if not res:
        # Address not found
        raise GeolocationException("Geolocalização de "
            "endereço não encontrado ({})".format(address))
    return Point(res.longitude, res.latitude)

def geolocate_cep(cep):
    if not re.search(cep_match, cep):
        raise ValueError("CEP inválido.")
    return geolocate(cep_address(cep))

def lookup_udh(cep):
    geoloc = geolocate_cep(cep)
    gdf = gpd.read_file(udh_shapefile)
    for shape in gdf.itertuples(index=False):
        if geoloc.within(shape.geometry):
            return shape.UDH_ATLAS
    return None