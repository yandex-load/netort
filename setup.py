from setuptools import setup, find_packages

setup(
    name='netort',
    version='0.0.11',
    description='common library for yandex-load org',
    longer_description='''
common library for yandex-load org
''',
    maintainer='Timur Torubarov (load testing)',
    maintainer_email='netort@yandex-team.ru',
    url='http://github.com/yandex-load/netort',
    packages=find_packages(exclude=["tests", "tmp", "docs", "data"]),
    install_requires=[
        'pyserial', 'requests', 'cerberus'
    ],
    setup_requires=[
        # 'pytest-runner', 'flake8',
    ],
    tests_require=[
        # 'pytest',
    ],
    license='LGPLv2',
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
    },
    package_data={
    },
    use_2to3=False, )
