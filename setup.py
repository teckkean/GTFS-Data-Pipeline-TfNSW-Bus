from setuptools import setup

setup(
    name='gtfs_dpl',
    version='0.0.1',
    packages=['gtfs_dpl'],
    install_requires=[
        'pandas',
        'geopandas',
        'zipfile',
        'json',
        'gzip',
        'flat_table'
        'pytz'
    ],
    package_data={
    # Include any *.pb.gz files in this subdirectory of the package:
        "gtfs_dpl": ["example_data/10_Raw_PB/2010m09d01_0800-0805/*.pb.gz"],
    }
)
