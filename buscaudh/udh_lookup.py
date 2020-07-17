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
_addr_cache = pd.read_csv(_data_root / 'all_cep_to_addr.csv',
    dtype={"complemento":str})
_geoloc_caches = tuple((gc, pd.read_csv(_data_root/'all_{}_locations'\
    '.csv'.format(gc))) for gc in ("custom", "osm", "arcgis"))

# format the CEP
def fix_cep(cep):
    cep_fmt = re.search(cep_match, ccc)
    if not cep_fmt:
        raise CEPException("CEP inválido:"
            " {}".format(cep))
    return "{}{}-{}".format(*cep_fmt.group(1,2,3))

#
#  CEP TO ADDR
#

# This function will load the cep_to_addr cache
def load_cep_to_addr_cache(filename = 'local_cep_to_addr.csv'):
    _cep_to_addr_cache = {}
    addr_keys = ['logradouro','complemento','bairro','cidade','estado','longitude','latitude']
    
    try:
      _addr_cache = pd.read_csv(_data_root / 'all_cep_to_addr.csv',
          dtype={"complemento":str})
      for ii in _addr_cache.index:
          cep_key = _addr_cache.at[ii,'cep']
          _cep_to_addr_cache[cep_key] = {}
          for key in addr_keys:
              _cep_to_addr_cache[cep_key][key] = _addr_cache.at[ii,key]
    except:
      pass

    try:
      _addr_cache = pd.read_csv(filename,
      dtype={"complemento":str})
      for ii in _addr_cache.index:
          cep_key = _addr_cache.at[ii,'cep']
          _cep_to_addr_cache[cep_key] = {}
          for key in addr_keys:
              _cep_to_addr_cache[cep_key][key] = _addr_cache.at[ii,key]
    except:
        # local cache with issues or nonexistent
      pass

    return _cep_to_addr_cache

# This function will save the cep_to_addr cache to a local file
def save_cep_to_addr_cache(cep_to_addr_cache,filename = 'local_cep_to_addr.csv'):
    local_cache = {'cep':list(cep_to_addr_cache.keys())}
    addr_keys = ['logradouro','complemento','bairro','cidade','estado','longitude','latitude']
    for key in addr_keys:
        local_cache[key] = [cep_to_addr_cache[ii][key] for ii in local_cache['cep']]
    pd.DataFrame(local_cache).to_csv(filename,index=False)

        
# This function will return the address associated with the
# CEP code. If not in cache, will try to solve the address
# from CORREIOS webservice
def cep_to_addr(cep, cep_to_addr_cache = [], retry=0, max_retries=1, use_correios = True, replace_cache = False):
    
    cep = fix_cep(cep)

    # If cache exists and will not be updated
    # If cep is in cache, return the addr
    if(cep_to_addr_cache) or (not replace_cache):
        if(cep in cep_to_addr_cache):
            return cep_to_addr_cache[cep]

    # If not to use correios webservice and not in cache
    # throw exception
    if(not use_correios):
        raise CEPException(def_msg="not in cache") from ex

    # will try to get info from correios
    # if error, throw exception
    try:
        client = zeep.Client(wsdl=_correios_wsdl)
    except (requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError):
        if retry < max_retries:
            return cep_to_addr(cep, retry+1)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        raise CEPException(def_msg="not found") from ex

    try:
        info = client.service.consultaCEP(cep)
    except (requests.exceptions.ConnectionError,
        zeep.exceptions.TransportError) as ex:
        if retry < max_retries:
            return cep_to_addr(cep, retry+1)
        raise CEPException(def_msg="connection error") from ex
    except zeep.exceptions.Fault as ex:
        # CEP not found
        raise CEPException(def_msg="not found") from ex
    if not info:
        raise CEPException(def_msg="not found")

    # will extract the avarege number of the street
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
    if(cep_to_addr_cache):
        cep_to_addr_cache[cep] = return_object

    return return_object, cep_to_addr_cache


#
#  ADDR TO GEO
#


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

def _key_addr(addr):
    addr_keys = ['logradouro','complemento','bairro','cidade','estado']
    return [addr[ii] for ii in addr_keys]


# This function will load the addr_to_geo cache
def load_addr_to_geo_cache(filename = 'local_addr_to_geo.csv'):
    _addr_to_geo_cache = {}
    addr_keys = ['logradouro','complemento','bairro','cidade','estado']
    geo_keys = ['longitude','latitude']
    
    try:
        _geo_cache = pd.read_csv(_data_root / 'all_addr_to_geo.csv')
        for ii in _geo_cache.index():
            addr_key = list(_geo_cache.at[ii,addr_keys])
            _addr_to_geo_cache[addr_key] = {}
            for key in geo_keys:
                _addr_to_geo_cache[addr_key][key] = _geo_cache.at[ii,key]
    except:
        pass

    try:
        _geo_cache = pd.read_csv(filename)
        for ii in _geo_cache.index():
            addr_key = list(_geo_cache.at[ii,addr_keys])
            _addr_to_geo_cache[addr_key] = {}
            for key in geo_keys:
                _addr_to_geo_cache[addr_key][key] = _geo_cache.at[ii,key]
    except:
        # local cahce with issues or nonexistent
        pass

    return _addr_to_geo_cache

# This function will save the addr_to_addr cache to a local file
def save_addr_to_geo_cache(addr_to_geo_cache,filename = 'local_addr_to_geo.csv'):
    local_cache = {'addr':list(addr_to_geo_cache.keys())}
    addr_keys = ['logradouro','complemento','bairro','cidade','estado']
    geo_keys = ['longitude','latitude']
    for key in geo_keys:
        local_cache[key] = [addr_to_geo_cache[ii][key] for ii in local_cache['addr']]
    pd.DataFrame(local_cache).to_csv(filename,index=False)


# Convert an add to a geo using a code system
def addr_to_geo(address, addr_to_geo_cache = [], gc_cod="osm"):

    if (addr_to_geo_cache):
        addr_ind = _key_addr(address)
        if(addr_ind in addr_to_geo_cache):
            res = addr_to_geo_cache[addr_ind]
            address["longitude"] = res.longitude
            address["latitude"] = res.latitude

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
        if gc_cod == "osm":
            return geolocate(address, "arcgis")
        raise GeolocationException("Geolocalização de "
            "endereço não encontrado ({})".format(search_addr))

    address["longitude"] = res.longitude
    address["latitude"] = res.latitude

    if(addr_to_geo_cache):
        addr_to_geo_cache[address]

    return address, addr_to_geo_cache




#
# CEP to GEO
#


# This function will load the cep_to_geo cache
def load_cep_to_geo_cache(filename = 'local_cep_to_geo.csv'):
    _cep_to_geo_cache = {}
    geo_keys = ['longitude','latitude']
    
    try:
        _geo_cache = pd.read_csv(_data_root / 'all_cep_to_geo.csv')
        for ii in _geo_cache.index():
            cep_key = _geo_cache.at[ii,'cep']
            _cep_to_geo_cache[cep_key] = {}
            for key in geo_keys:
                _cep_to_geo_cache[cep_key][key] = _geo_cache.at[ii,key]
    except:
        pass
    try:
        _geo_cache = pd.read_csv(filename)
        for ii in _geo_cache.index():
            cep_key = _geo_cache.at[ii,'cep']
            _cep_to_geo_cache[cep_key] = {}
            for key in geo_keys:
                _cep_to_geo_cache[cep_key][key] = _geo_cache.at[ii,key]
    except:
        # local cahce with issues or nonexistent
        pass

    return _cep_to_geo_cache

# This function will save the cep_to_addr cache to a local file
def save_cep_to_geo_cache(cep_to_geo_cache,filename = 'local_cep_to_geo.csv'):
    local_cache = {'cep':list(cep_to_geo_cache.keys())}
    geo_keys = ['longitude','latitude']
    for key in geo_keys:
        local_cache[key] = [cep_to_geo_cache[ii][key] for ii in local_cache['cep']]
    pd.DataFrame(local_cache).to_csv(filename,index=False)


# TODO
def cep_to_geo(cep, cep_to_geo_cache = []):

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
            addr = cep_to_addr(cep)
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
        geoloc = addr_to_geo(addr)
    except GeolocationException:
        return addr
    return geoloc



#
# CEP to UDH
#


# This function will load the cep_to_addr cache
def load_cep_to_udh_cache(filename = 'local_cep_to_udh.csv'):
    _cep_to_udh_cache = {}
    udh_keys = ['udh','longitude','latitude']
    try:
        _udh_cache = pd.read_csv(_data_root / 'all_cep_to_udh.csv')
        for ii in _udh_cache.index():
            cep_key = _udh_cache.at[ii,'cep']
            _cep_to_udh_cache[cep_key] = {}
            for key in udh_keys:
                _cep_to_udh_cache[cep_key][key] = _udh_cache.at[ii,key]
    except:
        pass
    try:
        _udh_cache = pd.read_csv(filename)
        for ii in _udh_cache.index():
            cep_key = _udh_cache.at[ii,'cep']
            _cep_to_udh_cache[cep_key] = {}
            for key in udh_keys:
                _cep_to_udh_cache[cep_key][key] = _udh_cache.at[ii,key]
    except:
        # local cahce with issues or nonexistent
        pass

    return _cep_to_udh_cache

# This function will save the cep_to_addr cache to a local file
def save_cep_to_udh_cache(cep_to_udh_cache,filename = 'local_cep_to_udh.csv'):
    local_cache = {'cep':list(cep_to_udh_cache.keys())}
    udh_keys = ['udh','longitude','latitude']
    for key in udh_keys:
        local_cache[key] = [cep_to_udh_cache[ii][key] for ii in local_cache['cep']]
    pd.DataFrame(local_cache).to_csv(filename,index=False)


# Main function to look for UDH code based on CEP code
def cep_to_udh(cep, cep_to_udh_cache = []):

    # will check the cache
    if(cep_to_udh_cache):
        if(cep in cep_to_udh_cache):
            return cep_to_udh_cache[cep]

    # will get the geolocation of the CEP code
    geoloc = cep_to_geo(cep)

    # if no location available, return None 
    if not geoloc.get("latitude") or not geoloc.get("longitude"):
        geoloc["udh"] = None
        return geoloc

    # if location available, will check if within limits of one UDH
    p = Point(geoloc["longitude"], geoloc["latitude"])
    udh = None
    for shape in _udh_gdf.itertuples(index=False):
        if p.within(shape.geometry):
            udh = shape.UDH_ATLAS
            break
    geoloc["udh"] = udh

    if(cep_to_udh_cache):
        cep_to_udh_cache[cep] = geoloc

    return geoloc, cep_to_udh_cache


