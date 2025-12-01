"""Trading Bot package setup."""
from setuptools import setup, find_packages

setup(
    name="trading-bot",
    version="1.0.0",
    description="AI Trading Bot with ICT/SMC Strategy",
    author="Your Name",
    author_email="your.email@example.com",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.31.0",
        "websocket-client>=1.6.0",
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "trading-bot=trading_bot.bot:main",
        ],
    },
)
