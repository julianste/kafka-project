FROM python:3.7

COPY producers/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY consumers/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

CMD ["/bin/bash"]
