from setuptools import find_packages, setup
setup(
  name="industryclouddatapipeline",
  version="0.0.2",
  author="Data Pipeline Team",
  author_email="datapipeline@pwc.com",
  description="Spark streaming data pipeline",
  long_description="",
  long_description_content_type="text/markdown",
  url="https://pmohub.pwc.com/confluence/display/SC/03+Insurance+Sector+Cloud",
  packages=find_packages(),
  classifiers=[
  "Programming Language :: Python :: 3",
  "License :: OSI Approved :: MIT License",
  "Operating System :: OS Independent",
  ],
  install_requires= [
 'databricks_api',
 'pytest'
  ],
  python_requires='>=3.7',
  entry_points="""
    [dp]
    data_init_gen=orchestrator_utils.data_init_generic:init_process
    file_monitor=misc.file_monitor:file_watcher_process
    file_movement=misc.file_movement:util_process
    direct_ingestion=orchestrator_utils.dynamic_ingestion:init_process
    data_init=orchestrator_utils.data_initialization:initilization_process
    data_init_cda=orchestrator_utils.data_init_cda:initilization_process
    data_init_tsk=orchestrator_utils.task_init:initilization_process
"""
)