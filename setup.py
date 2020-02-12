from cx_Freeze import setup, Executable

executables = [Executable('get_rt_data.py')]

setup(name='get_rt_data',
      version='0.0.1',
      description='Программа для выгрузки и сравнения реализаций',
      executables=executables)