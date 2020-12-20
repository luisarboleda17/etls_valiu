import setuptools

PACKAGE_NAME = 'etl_operations'
PACKAGE_VERSION = '0.0.1'
REQUIRED_PACKAGES = [
    'beam_nuggets',
    'psycopg2'
]

setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
