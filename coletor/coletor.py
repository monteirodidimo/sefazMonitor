# SEFAZ Monitor — Coletor de Latência TCP
# Tratamento das informações será feito via Power BI (DAX)
# Este módulo é responsável apenas por coleta dos dados.

import socket
import time
import threading
import csv
import os
from datetime import datetime

MODE = 'SOCKET_ONLY'  # 'HYBRID' ativa coleta SOAP (v2 desse código)
INTERVALO = 120
CSV_PATH = '/home/vmsefaz/coletor/dados.csv'

# Testa da Sefaz NFe e MDFe.
# Teste Google, CloudFlare e Quad 9 para testar link de internet.
ALVOS = [
    ("nfe.svrs.rs.gov.br",     443, "NFE_RJ_SVRS"),
    ("nfe.fazenda.sp.gov.br",  443, "NFE_SP"),
    ("nfe.fazenda.mg.gov.br",  443, "NFE_MG"),
    ("nfe.sefaz.pe.gov.br",    443, "NFE_PE"),
    ("nfe.sefaz.ba.gov.br",    443, "NFE_BA"),
    ("nfe.svrs.rs.gov.br",     443, "NFE_PA_DF_SVRS"),
    ("mdfe.svrs.rs.gov.br",    443, "MDFE_NACIONAL"),  # MDFe tem autorizador único nacional
    ("8.8.8.8",                443, "CTRL_GOOGLE"),
    ("1.1.1.1",                443, "CTRL_CLOUDFLARE"),
    ("9.9.9.9",                443, "CTRL_QUAD9"),
]

def medir_latencia(host, porta, timeout=3):
    # Retorna None em qualquer falha — o DAX trata como TIMEOUT.
    inicio = time.time()
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, porta))
        sock.close()
        return round((time.time() - inicio) * 1000, 2)
    except:
        return None

def coletar(host, porta, nome, resultado, lock):
    ms = medir_latencia(host, porta)
    with lock:
        resultado.append({
            "coletado_em": datetime.now().isoformat(),
            "nome": nome,
            "latencia_ms": ms
        })

def gravar_csv(resultado):
    # Append — nunca sobrescreve histórico.
    arquivo_novo = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, 'a', newline='') as f:
        writer = csv.DictWriter(
            f, fieldnames=['coletado_em', 'nome', 'latencia_ms']
        )
        if arquivo_novo:
            writer.writeheader()
        writer.writerows(resultado)

print(f"Coletor iniciado. MODE={MODE} | Alvos: {len(ALVOS)}")

while True:
    resultado = []
    lock = threading.Lock()
    threads = []

    for host, porta, nome in ALVOS:
        t = threading.Thread(
            target=coletar,
            args=(host, porta, nome, resultado, lock)
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    gravar_csv(resultado)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {len(resultado)} medições gravadas")
    time.sleep(INTERVALO)