#!/usr/bin/env python

from setuptools import setup

setup(
    # Metadata
    name='daybreak',
    version='1.0.3',
    description='Python clone of daybreak key-value database: http://propublica.github.io/daybreak',
    url='https://github.com/jacobbridges/daybreak-py',
    download_url='https://github.com/jacobbridges/daybreak-py/archive/master.tar.gz',
    license='MIT',
    author='Stanley Kariuki',
    author_email='stanleykariuki@gmail.com',
    maintainer='Jacob Bridges',
    maintainer_email='vash0the0stampede@gmail.com',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database'
    ],

    # Dependencies
    install_requires=[
        'toolz',
        'nose'
    ],
    packages=['daybreak'],

)