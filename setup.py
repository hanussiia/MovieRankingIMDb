from setuptools import setup, find_packages

VERSION = '0.0.104'
DESCRIPTION = 'Final Project "FILM RANKING" for Python course mimuw'

setup(
    name="filmranking",
    version=VERSION,
    author="Hanna Bernikova",
    author_email="<hb448385@students.mimuw.edu.pl>",
    description=DESCRIPTION,
    license = "LICENSE.txt",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=['filmranking'],
    install_requires=['pandas', 'numpy'],
    entry_points={
        'console_scripts': ['filmranking=filmranking:main']},
    classifiers = [
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python", ]    
)