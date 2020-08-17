from setuptools import setup, find_namespace_packages
import re
import os
import sys
import codecs

with open('requirements.txt') as f:
    required = f.read().splitlines()

try:
    # https://stackoverflow.com/questions/30700166/python-open-file-error
    with codecs.open("README.md", 'r', errors='ignore') as file:
        readme_contents = file.read()

except Exception as error:
    readme_contents = ""
    sys.stderr.write("Warning: Could not open README.md due %s\n" % error)


setup(name='pyplatform-common',
      version='2020.8.1',
      description='Pyplatform-common package provides utility, file management and authentication functions for interacting with APIs and compute services.',
      long_description=readme_contents,
      long_description_content_type="text/markdown",
      url='https://github.com/mhadi813/pyplatform',
      author='Muhammad Hadi',
      author_email='mhadi813@gmail.com',
      license='BSD',
      classifiers=[
          "Development Status :: 4 - Beta",
          "Intended Audience :: Developers",
          "Topic :: Office/Business",
          "License :: OSI Approved :: BSD License",
          "Programming Language :: Python :: 3.6",
          "Programming Language :: Python :: 3.7",
          "Programming Language :: Python :: 3.8",
      ],
      packages=find_namespace_packages(include=['pyplatform.*']),
      install_requires=required,
      keywords="google API pivotal cloud functions",
      zip_safe=False)
