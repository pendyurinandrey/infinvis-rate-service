FROM python:3.13-slim

WORKDIR /usr/local/

ADD app app/
ADD Pipfile ./
ADD Pipfile.lock ./
RUN pip install pipenv
RUN pipenv install

CMD ["pipenv", "run", "server"]