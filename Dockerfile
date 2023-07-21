FROM continuumio/anaconda3

ADD ./config/requirements.txt /
ADD ./pm4py-core /pm4py

RUN apt-get update -y
RUN apt-get -y install gcc graphviz
RUN pip install --upgrade setuptools
RUN conda install -c conda-forge cvxopt
RUN pip install -r requirements.txt
RUN ["pip", "install", "/pm4py"]

EXPOSE 8888

CMD ["jupyter", "notebook", "--no-browser","--NotebookApp.token=''","--NotebookApp.password=''", "--allow-root"]