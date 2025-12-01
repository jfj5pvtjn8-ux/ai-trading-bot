FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY setup.py .
COPY src/ src/

# Install the package in editable mode
RUN pip install -e .

# Copy config and other files
COPY config/ config/
COPY .env .env

# Expose health check port
EXPOSE 8080

CMD ["python", "-m", "trading_bot.bot"]
