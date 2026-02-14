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

# Limite para NÃO incluir cobranças altas (ex: robô)
# Você pode sobrescrever via variável/secret: MAX_PAYMENT_VALUE
LIMITE_VALOR = float(os.environ.get("MAX_PAYMENT_VALUE", "1000"))

if not API_KEY:
    raise Exception("API Key não configurada. Configure ASAAS_API_KEY nas variáveis de ambiente.")

HEADERS = {
    "access_token": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "mecanicaautomation/1.0"
}

# IMPORTANTÍSSIMO:
# No GitHub Actions (Linux), caminhos Windows não funcionam.
# Então usamos uma pasta local do repo por padrão, mas mantemos compatível com Windows.
DEFAULT_EXPORT_PATH = os.path.join(os.getcwd(), "export")
DEFAULT_LOG_PATH = os.path.join(os.getcwd(), "logs")

EXPORT_PATH = os.environ.get("EXPORT_PATH", DEFAULT_EXPORT_PATH)
LOG_PATH = os.environ.get("LOG_PATH", DEFAULT_LOG_PATH)

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
# EXPORTAR PARA EXCEL (com filtro de valor)
# ==============================
def exportar_excel(dados: list[dict]) -> str | None:
    os.makedirs(EXPORT_PATH, exist_ok=True)

    if not dados:
        msg = "Nenhuma cobrança vencida encontrada (status=OVERDUE)."
        log(msg)
        print(msg)
        return None

    # ✅ FILTRO: só valores abaixo do limite
    dados_filtrados: list[dict] = []
    pulados_acima = 0

    for item in dados:
        try:
            valor = float(item.get("value") or 0)
        except (TypeError, ValueError):
            valor = 0.0

        if valor < LIMITE_VALOR:
            dados_filtrados.append(item)
        else:
            pulados_acima += 1

    log(f"Filtro aplicado: mantendo valores < R$ {LIMITE_VALOR:.2f}. Pulados (>= limite): {pulados_acima}")
    print(f"Filtro aplicado: mantendo valores < R$ {LIMITE_VALOR:.2f}. Pulados (>= limite): {pulados_acima}")

    if not dados_filtrados:
        msg = f"Nenhuma cobrança vencida abaixo de R$ {LIMITE_VALOR:.2f}."
        log(msg)
        print(msg)
        return None

    linhas = []
    for item in dados_filtrados:
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

    # ✅ trava de segurança extra
    if "Valor" in df.columns:
        df["Valor"] = pd.to_numeric(df["Valor"], errors="coerce").fillna(0.0)
        df = df[df["Valor"] < LIMITE_VALOR].copy()

    if df.empty:
        msg = f"Após filtro/trava, não restou nenhuma cobrança abaixo de R$ {LIMITE_VALOR:.2f}."
        log(msg)
        print(msg)
        return None

    if "Vencimento" in df.columns:
        # Vencimento pode ser string, mas ordena ok.
        df = df.sort_values(by=["Vencimento", "Valor"], ascending=[True, False])

    nome_arquivo = f"vencidos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    caminho = os.path.join(EXPORT_PATH, nome_arquivo)
    df.to_excel(caminho, index=False)

    log(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)} | Limite: < R$ {LIMITE_VALOR:.2f}")
    print(f"Arquivo gerado: {nome_arquivo} | Registros: {len(df)} | Limite: < R$ {LIMITE_VALOR:.2f}")
    return caminho

# ==============================
# EXECUÇÃO PRINCIPAL
# ==============================
def main() -> None:
    log("===== INICIO JOB ASAAS VENCIDOS =====")
    print("Iniciando job...")

    log(f"Config: BASE_URL={BASE_URL} | LIMITE_VALOR={LIMITE_VALOR:.2f} | EXPORT_PATH={EXPORT_PATH}")
    print(f"Config: LIMITE_VALOR={LIMITE_VALOR:.2f} | EXPORT_PATH={EXPORT_PATH}")

    dados = buscar_vencidos()
    print("Qtd vencidos encontrados (total):", len(dados))
    log(f"Qtd vencidos encontrados (total): {len(dados)}")

    arquivo = exportar_excel(dados)

    # Envia e-mail somente se gerou arquivo
    if arquivo:
        enviar_email_com_anexo(
            assunto="Asaas - Clientes Vencidos (somente < R$ 1.000)",
            corpo=f"Segue planilha em anexo com os títulos vencidos abaixo de R$ {LIMITE_VALOR:.2f}.",
            destinatarios=[
                "vendas@mecanicaweb.com.br",
                                
            ],
            arquivo=arquivo
        )

        log("E-mail enviado com anexo.")
        print("E-mail enviado com anexo.")
    else:
        log("Sem arquivo para enviar por e-mail (sem vencidos no critério).")
        print("Sem arquivo para enviar por e-mail (sem vencidos no critério).")

    log("===== FIM JOB ASAAS VENCIDOS =====")
    print("Job finalizado.")

if __name__ == "__main__":
    main()
