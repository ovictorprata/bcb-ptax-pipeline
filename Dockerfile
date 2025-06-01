FROM apache/airflow:2.10.2          
COPY requirements.txt .

# Usa o arquivo de constraints da versão para evitar conflitos
RUN pip install --no-cache-dir -r requirements.txt 
