from setuptools import setup, find_packages

setup(
    name='austxt',
    version='0.1',
    py_modules=['austxt'],
    install_requires=[
        'click',
        'lxml',
        'unidecode',
        'pandas',
        'spacy',
        'en_core_web_sm',
        'elasticsearch'
    ],
    dependency_links=['https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-2.0.0/en_core_web_sm-2.0.0.tar.gz#egg=en_core_web_sm-2.0.0'],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    entry_points={'console_scripts': [
        'austxt=austxt.cli:cli',
    ]}
)
