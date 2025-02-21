from setuptools import setup, find_packages 

setup(
    name= "entregables_2025",
    author= "Laura Arboleda",
    version= "0.0.1",
    author_email= "",
    description= "", 
    py_modules= ["entregables"],
    install_requires = [
        "pandas",
        "openpyxl",
        "requests"
    ]
)