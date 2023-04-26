FROM httpd

ENV VIRTUAL_ENV=/usr/local/apache2/cgi-bin/spcommsV3/venv

RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip python3-venv
#apt-get clean && \
#rm -rf /var/lib/apt/lists/*

#RUN apt-get update 
#RUN apt-get install -y  python3
#RUN apt-get install -y  apache2-dev
RUN apt-get install -y  vim systemctl net-tools procps
#RUN apt-get install -y  pip
#RUN apt-get install -y  python3-venv
RUN python3 -m venv /usr/local/apache2/cgi-bin/spcommsV3/venv

RUN python3 -m venv $VIRTUAL_ENV
COPY Requirements_cgi.txt /tmp
RUN . ${VIRTUAL_ENV}/bin/activate && pip install -r /tmp/Requirements_cgi.txt

RUN apt-get clean
RUN apt-get autoremove --purge
RUN rm -rf /var/lib/apt/lists/*

COPY config/ /usr/local/apache2/cgi-bin/config/
COPY FUNCAUX/ /usr/local/apache2/cgi-bin/FUNCAUX/
COPY spcomms.py /usr/local/apache2/cgi-bin/

COPY index.html /usr/local/apache2/htdocs/index.html


