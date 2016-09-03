
from setuptools import setup, find_packages
from pip.req import parse_requirements
import sys, os

install_reqs = parse_requirements("requirements.txt")

setup(name='mmw_discogs',
    version='0.0.1',
    description="manymanywomen discogs parser",
    classifiers=[],
    keywords='',
    author='scztt',
    author_email='scott@artificia.org',
    url='',
    license='MIT',
    packages=['mmw_discogs'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'beautifulsoup4',
        'discogs_client',
        'fuzzywuzzy',
        'atomicfile'
    ],
    setup_requires=[],
    entry_points={
        'console_scripts': [
            'mmw_discogs = mmw_discogs:run'
        ]
    },
    namespace_packages=[],
)
