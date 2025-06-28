"""
Exemplo de DAG com diferentes formas de escalar em pods
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.kubernetes.operators.pod import KubernetesPodOperator
from airflow.configuration import conf

# Configurações padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Criando o DAG
dag = DAG(
    'exemplo_escalavel',
    default_args=default_args,
    description='Exemplo de tasks escaláveis em pods',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['exemplo', 'kubernetes', 'escalavel']
)

# 1. Task tradicional (roda no worker/scheduler)
def task_simples():
    print("Esta task roda no pod worker tradicional")
    # Simulando processamento
    import time
    time.sleep(10)
    return "Task concluída!"

task_tradicional = PythonOperator(
    task_id='task_tradicional',
    python_callable=task_simples,
    dag=dag
)

# 2. Task com KubernetesPodOperator - Mais controle
task_kubernetes_pod = KubernetesPodOperator(
    task_id='task_kubernetes_custom',
    name='airflow-task-custom',
    namespace='airflow-app',
    image='python:3.9',
    cmds=['python'],
    arguments=['-c', '''
import time
print("Executando em pod customizado!")
print("Processando dados...")
time.sleep(15)
print("Processamento concluído!")
    '''],
    # Recursos específicos para esta task
    resources={
        'request_memory': '128Mi',
        'request_cpu': '100m',
        'limit_memory': '512Mi',
        'limit_cpu': '500m'
    },
    # Labels para identificação
    labels={'app': 'airflow-custom-task', 'version': 'v1'},
    # Configurações de restart
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# 3. Task para processamento pesado - pod com mais recursos
task_processamento_pesado = KubernetesPodOperator(
    task_id='task_processamento_pesado',
    name='airflow-heavy-processing',
    namespace='airflow-app',
    image='python:3.9',
    cmds=['python'],
    arguments=['-c', '''
import time
import random

print("Iniciando processamento pesado...")
# Simulando processamento que precisa de mais recursos
for i in range(10):
    print(f"Processando batch {i+1}/10...")
    time.sleep(2)
    
print("Processamento pesado concluído!")
    '''],
    # Recursos maiores para processamento pesado
    resources={
        'request_memory': '512Mi',
        'request_cpu': '200m',
        'limit_memory': '1Gi',
        'limit_cpu': '1000m'
    },
    # Política de restart
    restart_policy='Never',
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# 4. Task em paralelo para demonstrar escalabilidade
def criar_task_paralela(task_id, numero):
    return KubernetesPodOperator(
        task_id=f'task_paralela_{numero}',
        name=f'airflow-parallel-{numero}',
        namespace='airflow-app',
        image='python:3.9',
        cmds=['python'],
        arguments=['-c', f'''
import time
import random

print("Task paralela {numero} iniciada")
# Cada task tem duração aleatória para simular trabalho real
sleep_time = random.randint(5, 15)
print(f"Processando por {{sleep_time}} segundos...")
time.sleep(sleep_time)
print("Task paralela {numero} concluída!")
        '''],
        resources={
            'request_memory': '64Mi',
            'request_cpu': '50m',
            'limit_memory': '256Mi',
            'limit_cpu': '200m'
        },
        is_delete_operator_pod=True,
        get_logs=True,
        dag=dag
    )

# Criando 5 tasks paralelas
tasks_paralelas = [criar_task_paralela(f'paralela_{i}', i) for i in range(1, 6)]

# 5. Task de Machine Learning exemplo
task_ml_exemplo = KubernetesPodOperator(
    task_id='task_ml_exemplo',
    name='airflow-ml-job',
    namespace='airflow-app',
    image='python:3.9',
    cmds=['sh'],
    arguments=['-c', '''
pip install pandas numpy scikit-learn --quiet
python -c "
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

print('Gerando dados sintéticos...')
# Dados sintéticos
np.random.seed(42)
X = np.random.randn(1000, 5)
y = X.sum(axis=1) + np.random.randn(1000) * 0.1

print('Treinando modelo...')
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = LinearRegression()
model.fit(X_train, y_train)

print('Avaliando modelo...')
predictions = model.predict(X_test)
mse = mean_squared_error(y_test, predictions)
print(f'MSE: {mse:.4f}')
print('Modelo treinado com sucesso!')
"
    '''],
    resources={
        'request_memory': '256Mi',
        'request_cpu': '200m',
        'limit_memory': '1Gi',
        'limit_cpu': '500m'
    },
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag
)

# Definindo dependências
task_tradicional >> task_kubernetes_pod
task_kubernetes_pod >> tasks_paralelas
task_kubernetes_pod >> task_processamento_pesado
task_processamento_pesado >> task_ml_exemplo 