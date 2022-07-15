FROM continuumio/anaconda3

ADD ./config/requirements.txt /

RUN apt-get update -y
RUN apt-get -y install gcc
RUN pip install --upgrade setuptools
RUN pip install -r requirements.txt

RUN mkdir -p /root/.jupyter

EXPOSE 8888

CMD ["jupyter", "notebook", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''"]