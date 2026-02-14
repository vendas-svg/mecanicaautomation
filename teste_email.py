# teste_email.py
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

def enviar_email_com_anexo(
    assunto: str,
    corpo: str,
    destinatarios: list[str],
    arquivo: str | None = None,
) -> None:
    smtp_host = os.environ["SMTP_HOST"]
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = ", ".join(destinatarios)
    msg["Subject"] = assunto
    msg.set_content(corpo)

    if arquivo:
        p = Path(arquivo)
        with p.open("rb") as f:
            data = f.read()
        msg.add_attachment(
            data,
            maintype="application",
            subtype="octet-stream",
            filename=p.name
        )

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.ehlo()
        s.starttls()
        s.ehlo()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

# Teste manual (só roda se você executar: python teste_email.py)
if __name__ == "__main__":
    enviar_email_com_anexo(
        assunto="Teste SMTP - MecanicaAutomation",
        corpo="Se você recebeu este e-mail, o SMTP está funcionando.",
        destinatarios=[os.environ["SMTP_USER"]],
        arquivo=None,
    )
    print("FINALIZOU: Email enviado com sucesso!")





