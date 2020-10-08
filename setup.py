from setuptools import setup, find_packages

setup(
    name="files-airflow-kubernetes",
    version="0.1",
    description="Meltano project files for Airflow with KubernetesPodOperator",
    packages=find_packages(),
    package_data={"bundle": ["orchestrate/dags/meltano.py"]},
)
