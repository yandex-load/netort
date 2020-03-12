from setuptools import setup, find_packages

setup(
    name='netort',
    version='0.7.8',
    description='common library for yandex-load org',
    longer_description='''
common library for yandex-load org
''',
    author='Timur Torubarov (load testing)',
    author_email='netort@yandex-team.ru',
    maintainer='Yandex load team',
    maintainer_email='load@yandex-team.ru',
    url='http://github.com/yandex-load/netort',
    packages=find_packages(exclude=["tests", "tmp", "docs", "data"]),
    install_requires=[
        'pyserial', 'requests', 'retrying', 'cerberus', 'six>=1.12.0', 'pandas>=0.23.0',
        'pathlib', 'typing', 'numpy'
    ],
    setup_requires=[
        # 'pytest-runner', 'flake8',
    ],
    tests_require=[
        'pytest', 'pytest-runner', 'mock'
    ],
    license='MPLv2',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU Lesser General Public License v2 or later (LGPLv2+)',
        'Operating System :: POSIX',
        'Topic :: Software Development :: Quality Assurance',
        'Topic :: Software Development :: Testing',
        'Topic :: Software Development :: Testing :: Traffic Generation',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
    entry_points={
        'console_scripts': [
            'phout_upload = netort.cli:main',
        ],
    },
    package_data={
    },
    use_2to3=False, )
