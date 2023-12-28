FROM python:3.8-slim-buster

WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 80

CMD ["tail", "-f", "/dev/null"]