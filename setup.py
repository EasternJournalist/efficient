from setuptools import setup, find_packages

setup(
    name='efficient',
    version='0.1.0',
    description='Python concurrency',
    url='https://github.com/EasternJournalist/efficient',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.7',
)