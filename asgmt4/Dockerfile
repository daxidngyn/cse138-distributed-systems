FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY server_new.py paxos.py requirements.txt ./

CMD [ "python3", "./server_new.py" ]
EXPOSE 8090