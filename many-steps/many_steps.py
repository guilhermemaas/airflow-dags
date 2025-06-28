from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="* * * * *",  # a cada 1 minuto
    catchup=False,
    tags=["exemplo", "teste"]
)
def hello_world_pipeline():
    @task
    def step_1():
        print("🔧 Etapa 1: Preparando dados...")

    @task
    def step_2():
        print("📊 Etapa 2: Processando...")

    @task
    def step_3():
        print("✅ Etapa 3: Salvando resultado...")

    @task
    def step_4():
        print("🚀 Etapa 4: Finalizado!")

    # Definir ordem de execução
    step_1() >> step_2() >> step_3() >> step_4()

gmaas_many_steps_dag = hello_world_pipeline()
