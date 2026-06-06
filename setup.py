from setuptools import setup, find_packages
setup(
    name='ali2026v3_trading',
    version='7.1.0',
    packages=find_packages(),
    install_requires=[
        'duckdb>=0.8,<2.0',
        'pandas>=1.5,<3.0',
        'pyarrow>=12.0,<25.0',
        'numpy>=1.21,<3.0',
        'PyYAML>=6.0',
        'optuna>=3.0',
        'scipy>=1.9',
    ],
    python_requires='>=3.10',
)
