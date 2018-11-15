from setuptools import setup, find_packages

setup(
    name='austxt',
    version='0.1',
    py_modules=['austxt'],
    install_requires=[
        'click',
        'lxml',
        'unidecode',
        'pandas'
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    #package_data={'austxt': ['data/*.csv']},
    entry_points={'console_scripts': [
        'austxt=austxt.cli:cli',
    ]}
)
