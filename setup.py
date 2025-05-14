from setuptools import setup, find_packages

setup(
    name="pyeztrace",
    version="0.0.1",
    description="Python tracing and logging library",
    author="Jefferson Nelsson",
    packages=find_packages(),
    install_requires=["pydantic-core>=2.33.2", "pydantic-settings>=2.9.1"],
    python_requires=">=3.7",
)
