from http.client import NON_AUTHORITATIVE_INFORMATION


class DMTempo():
    def __init__(self,id_tempo,nu_ano,nu_mes,nu_anomes,sg_mes,nm_mesano,nm_mes,nu_dia) -> None:
        self.id_tempo = id_tempo
        self.nu_ano = nu_ano
        self.nu_mes = nu_mes
        self.nu_anomes = nu_anomes
        self.sg_mes = sg_mes
        self.nm_mesano = nm_mesano
        self.nm_mes = nm_mes
        self.nu_dia = nu_dia
