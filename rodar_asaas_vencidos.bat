@echo off
cd /d C:\Tanigawa\mecanicaautomation
call venv\Scripts\activate
python asaas_vencidos.py >> logs\job_asaas_vencidos.log 2>&1
