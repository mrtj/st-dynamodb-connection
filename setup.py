import setuptools

VERSION = "0.0.1"  # PEP-440

NAME = "streamlit_dynamodb"

INSTALL_REQUIRES = [
    "streamlit>=1.22.0",
    "boto3>=1.28.9",
    "pandas>=2.0.3",
    "dynamodb-mapping>=0.1.0",
]

setuptools.setup(
    name=NAME,
    version=VERSION,
    description="Streamlit Connection for Amazon DynamoDB.",
    url="https://github.com/mrtj/st-dynamodb-connection",
    project_urls={
        "Source Code": "https://github.com/mrtj/st-dynamodb-connection",
    },
    author="Janos Tolgyesi",
    author_email="janos.tolgyesi@gmail.com",
    license="Apache License 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.8",
    # Requirements
    install_requires=INSTALL_REQUIRES,
)
