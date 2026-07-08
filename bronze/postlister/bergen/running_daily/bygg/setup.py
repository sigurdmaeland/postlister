from setuptools import setup, find_packages

setup(
    name="postlister-bergen-bygg-bronze",
    version="0.2.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "aiohttp>=3.9.0",
        "requests>=2.31.0",
        "psycopg2-binary>=2.9.6"
    ],
    entry_points={
    "console_scripts": [
        "bergen-bygg-trigger=app.main:trigger",
    ]
},
    python_requires=">=3.10",
)
