import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mongodb_wrapper",
    version="0.0.1",
    author="Ismael Raya",
    author_email="phornee@gmail.com",
    description="DB management wrapper over mariaDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Phornee/mongodb_wrapper",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pymongo>=3.11.3',
        'dnspython>=2.1.0',
    ],
    python_requires='>=3.6',
)