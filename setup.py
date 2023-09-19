import setuptools


with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="edacious",
    version="0.0.8",
    author="Eldad Bishari",
    author_email="eldad@1221tlv.org",
    description="Edacious an Event Derive Architecture framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eldad1221/edacious",
    packages=setuptools.find_packages(),
    install_requires=[
        'quickbelog>=1.1.0',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)