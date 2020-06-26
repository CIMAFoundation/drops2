import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="drops2",
    version="0.5.0",
    author="Mirko D'Andrea",
    author_email="mirko.dandrea@cimafoundation.org",
    description="dds data access api",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CIMAFoundation/drops2",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'decorator',
        'geopandas',
        'numpy',
        'pandas',
        'requests',
        'scipy',
        'xarray',
      ],
)