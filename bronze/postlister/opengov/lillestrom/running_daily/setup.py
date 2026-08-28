from setuptools import setup, find_packages

setup(
    name="postlister-lillestrom-bronze",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "requests>=2.31.0",
        "beautifulsoup4>=4.12.0",
        "azure-storage-blob>=12.19.0",
        "azure-identity>=1.15.0",
        "python-dotenv>=1.0.0"
    ],
    entry_points={
        "console_scripts": [
            "lillestrom-bygg-trigger=app.main:trigger_bygg",
            "lillestrom-henv-trigger=app.main:trigger_henv",
            "lillestrom-ulov-trigger=app.main:trigger_ulov",
            "lillestrom-tilsyn-trigger=app.main:trigger_tilsyn",
        ]
    },
    python_requires=">=3.10",
)
