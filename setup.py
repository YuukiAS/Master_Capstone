from setuptools import setup, find_packages

setup(
    name="BIOS992",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        # Database
        "sqlite3",  # 通常是Python标准库的一部分
        
        # Data processing
        "pandas",
        "numpy",
        "dask",
        
        # ECG processing
        "neurokit2",
        "xmltodict",
        
        # Progress bars
        "tqdm",
        
        # Visualization
        "matplotlib",
        "IPython",  # 用于notebook显示
    ],
    python_requires=">=3.9",  # 根据您的.ipynb文件中使用的Python 3.9.18版本
)