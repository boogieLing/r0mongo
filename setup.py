from setuptools import setup, find_packages

with open('README.rst', 'r') as fp:
    long_description = fp.read()

setup(
    name='r0mongo',
    version='0.1.1',
    description='Assist to complete the operations related to the addition, deletion, modification and query of '
                'mongodb, and provide rich time-related query operations',
    long_description=long_description,
    author='r0',
    author_email='boogieLing_o@163.com',
    license='MIT',
    url='https://github.com/boogieLing/r0mongo',
    download_url='https://github.com/boogieLing/r0mongo/tarball/v0.1.1',
    packages=find_packages(),
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
    install_requires=[

    ],
    include_package_data=True,
    package_dir={'': '.'},
    package_data={
        '': ['*.ini'],
    },
    entry_points={
        'console_scripts': [

        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development :: Libraries',
        'Intended Audience :: Developers',
    ],
    keywords=['mongo', 'dictionary', 'secondary index', 'model', 'prototype', 'helper']
)
