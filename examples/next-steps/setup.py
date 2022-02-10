"""
Example setup file for a project using Celery.

This can be used to distribute your tasks and worker
as a Python package, on PyPI or on your own private package index.

"""

from setuptools import find_packages, setup

setup(
    name='example-tasks',
    url='http://github.com/example/celery-tasks',
    author='Ola A. Normann',
    author_email='author@example.com',
    keywords='our celery integration',
    version='2.0',
    description='Tasks for my project',
    long_description=__doc__,
    license='BSD',
    packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
    test_suite='pytest',
    zip_safe=False,
    install_requires=[
        'celery>=5.0',
        #  'requests',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy3',
        'Operating System :: OS Independent',
    ],
)
