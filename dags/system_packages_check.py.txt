from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pkg_resources
from airflow.providers_manager import ProvidersManager

def list_installed_packages(**context):
    """Список всех Python-пакетов с версиями"""
    packages = sorted([(pkg.key, pkg.version) for pkg in pkg_resources.working_set])
    
    print("\n=== Все установленные пакеты ===")
    for pkg, version in packages:
        print(f"{pkg}=={version}")
    
    context['ti'].xcom_push(key='total_packages', value=len(packages))
    return packages

def list_airflow_providers(**context):
    """Список установленных провайдеров Airflow с версиями"""
    providers = ProvidersManager().providers
    
    print("\n=== Установленные провайдеры Airflow ===")
    for provider in sorted(providers.keys()):
        print(f"{provider}: {providers[provider].version}")
    
    # Проверка ключевых провайдеров
    critical_providers = {
        'apache-kafka': 'apache-airflow-providers-apache-kafka',
        'postgres': 'apache-airflow-providers-postgres',
        'http': 'apache-airflow-providers-http'
    }
    
    missing = []
    for name, pkg in critical_providers.items():
        try:
            version = pkg_resources.get_distribution(pkg).version
            print(f"\nПровайдер {name} ({pkg}) найден, версия: {version}")
        except pkg_resources.DistributionNotFound:
            print(f"\n⚠️ Провайдер {name} ({pkg}) НЕ УСТАНОВЛЕН!")
            missing.append(name)
    
    context['ti'].xcom_push(key='missing_providers', value=missing)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

with DAG(
    'system_packages_check',
    default_args=default_args,
    description='Проверка установленных пакетов и провайдеров Airflow',
    schedule_interval=None,
    catchup=False,
    tags=['debug', 'system'],
) as dag:

    list_packages_task = PythonOperator(
        task_id='list_all_packages',
        python_callable=list_installed_packages,
        provide_context=True
    )

    list_providers_task = PythonOperator(
        task_id='list_airflow_providers',
        python_callable=list_airflow_providers,
        provide_context=True
    )

    print_summary_task = PythonOperator(
        task_id='print_summary',
        python_callable=lambda **context: print(
            f"\nИтог:\n"
            f"Всего пакетов: {context['ti'].xcom_pull(task_ids='list_all_packages', key='total_packages')}\n"
            f"Отсутствующие провайдеры: {context['ti'].xcom_pull(task_ids='list_airflow_providers', key='missing_providers') or 'Все на месте!'}"
        )
    )

    list_packages_task >> list_providers_task >> print_summary_task