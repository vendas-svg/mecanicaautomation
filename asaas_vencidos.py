import os
import requests
import pandas as pd
from datetime import datetime

# Se você já tem teste_email.py com essa função, mantenha:
from teste_email import enviar_email_com_anexo

# ==============================
# DIAGNÓSTICO (pode deixar)
# ==============================
print("RODANDO:", __file__)
print("CWD:", os.getcwd())

# ==============================
# CONFIGURAÇÃO SEGURA
# ==============================
API_KEY = os.environ.get("ASAAS_API_KEY")
BASE_URL = os.environ.get("ASAAS_BASE_URL", "https://www.asaas.com/api/v3")

if not API_KEY:
    raise Exception("API Key não configurada. Configure ASAAS_API_KEY nas variáveis de ambiente.")

HEADERS = {
    "access_token": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "mecanicaautomation/1.0"
}

EXPORT_PATH = r"C:\Tanigawa\mecanicaautomation\export"
LOG_PATH = r"C:\Tanigawa\mecanicaautomation\logs"

# ==============================
# LOG
# ==============================
def log(message: str) -> None:
    os.makedirs(LOG_PATH, exist_ok=True)
    with open(os.path.join(LOG_PATH, "job.log"), "a", encoding="utf-8") as f:
        f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} - {message}\n")

# ==============================
# BUSCAR COBRANÇAS VENCIDAS (com paginação)
# ==============================
def buscar_vencidos(limit: int = 100) -> list[dict]:
    url = f"{BASE_URL}/payments"

    offset = 0
    todos: list[dict] = []

    while True:
        params = {
            "status": "OVERDUE",
            "limit": limit,
            "offset": offset,
        }

        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)

        if resp.status_code != 200:
            print("ERRO API:", resp.status_code, resp.text)
            log(f"Erro API: {resp.status_code} - {resp.text}")
            raise Exception(f"Erro ao consultar Asaas: HTTP {resp.status_code}")

        payload = resp.json()
        items = payload.get("data", [])

        if not items:
            break

        todos.extend(items)
        offset += limit

    return todos

# ==============================
# EXPORTAR PARA EXCEL
# ==============================
def exportar_excel(dados: list[dict]) -> str | None:
    os.makedirs(EXPORT_PATH, exist_ok=True)

    if not dados:
        log("Nenhuma cobrança vencida encontrada (status=OVERDUE).")
        print("Nenhuma cobrança vencida encontrada (status=OVERDUE).")
        return None

    linhas = []
    for item in dados:
        linhas.append({
            "ID": item.get("id"),
            "CustomerID": item.get("customer"),
            "Valor": item.get("value"),
            "Vencimento": item.get("dueDate"),
            "Tipo": item.get("billingType"),
            "Status": item.get("status"),
            "Descricao": item.get("description"),
            "ExternalReference": item.get("externalReference"),
            "InvoiceURL": item.get("invoiceUrl"),
            "BankSlipURL": item.get("bankSlipUrl"),
        })

    df = pd.DataFrame(linhas)

    if "Vencimento" in df.columns:
        df = df.sort_values(by=["Vencimento", "Valor"], ascending=[True, False])

    nome_arquivo = f"vencidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho = os.path.join(EXPORT_PATH, nome_arquivo)
    df.to_excel(caminho, index=False)

    log(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)}")
    print(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)}")
    return caminho

# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================
def main() -> None:
    log("===== INICIO JOB ASAAS VENCIDOS =====")
    print("Iniciando job...")

    dados = buscar_vencidos()
    print("Qtd vencidos encontrados:", len(dados))
    log(f"Qtd vencidos encontrados: {len(dados)}")

    arquivo = exportar_excel(dados)

    # Envia e-mail somente se gerou arquivo
    if arquivo:
        enviar_email_com_anexo(
        assunto="Asaas - Clientes Vencidos",
        corpo="Segue planilha em anexo com os títulos vencidos.",
        destinatarios=["vendas@mecanicaweb.com.br",
                       "suporte@istweb.com.br",
                       "marcelino@istweb.com.br"
                                              
                       ],
        arquivo=arquivo
        )

        
        log("E-mail enviado com anexo.")
        print("E-mail enviado com anexo.")
    else:
        log("Sem arquivo para enviar por e-mail (sem vencidos).")
        print("Sem arquivo para enviar por e-mail (sem vencidos).")

    log("===== FIM JOB ASAAS VENCIDOS =====")
    print("Job finalizado.")

if __name__ == "__main__":
    main()
