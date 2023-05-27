from distutils.core import setup
import setuptools

setup(name='mdd',
      version='1.0',
      packages=setuptools.find_packages(),
      description='Metadata driven framework built over top of pyspark',
      author='Aditya Saraswat,Alind Billore, Rishi Bhutada, Omkar Chaudhari',
      author_email='alind.billore@globant.com',
      url='<repo url>',
      requires=['jinja2', 'pyyaml'],
      python_requires=">=3.8",
      package_dir={"mdd": "mdd"},
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Independent"],
      )
