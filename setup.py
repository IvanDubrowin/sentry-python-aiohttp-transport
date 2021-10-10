from setuptools import setup, find_packages

from sentry_aiohttp_transport import __version__, __author__

setup(
    name='sentry_aiohttp_transport',
    version=__version__,
    author=__author__,
    description='Aiohttp transport for sentry-python sdk',
    long_description=open('README.md').read(),
    platforms='all',
    classifiers=[
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    packages=find_packages(exclude=['tests', 'examples']),
    install_requires=[
        'aiohttp>=3.7',
        'sentry-sdk>=1.4.3',
    ],
    extras_require={
        'develop': [
            'mypy',
            'wemake-python-styleguide',
            'pytest',
            'pytest-asyncio',
        ],
    },
    python_requires='>=3.9',
    project_urls={
        'Source': 'https://github.com/IvanDubrowin/sentry-python-aiohttp-transport',
    }
)
