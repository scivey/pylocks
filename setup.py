from setuptools import setup, find_packages

INSTALL_REQUIRES = [
    'redis>=2.10.5'
]

setup(
    name='pylocks',
    version='0.2.1',
    description="redis-backed locks and leases",
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3'
    ],
    install_requires=INSTALL_REQUIRES,
    author='Scott Ivey',
    author_email='scott.ivey@gmail.com',
    license='MIT',
    packages=find_packages()
)