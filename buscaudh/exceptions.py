class CEPException(Exception):
    def __init__(self, msg="", def_msg=None, *args, **kwargs):
        if def_msg == "not found":
            msg = "Falha na consulta de CEP:"\
                " CEP não encontrado."
        elif def_msg == "connection error":
            msg = "Falha de conexão"\
                "na consulta de CEP."
        super().__init__(msg, *args, **kwargs)

class GeolocationException(Exception):
    pass