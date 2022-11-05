FROM summerwind/actions-runner:latest

RUN sudo apt update -y \
    && sudo apt install -y software-properties-common \
    && sudo apt install -y poppler-utils python3.9 python3.9-dev \
    && sudo rm -rf /var/lib/apt/lists/*

ADD http://ftp.gnu.org/gnu/make/make-4.3.tar.gz /tmp/

RUN cd /tmp \
    && sudo tar -xzf make-4.3.tar.gz \
    && cd make-4.3 \
    && ./configure --prefix=/usr/local \
    && make \
    && sudo make install \
    && sudo rm -rf /tmp/make-4.3 /tmp/make-4.3.tar.gz

RUN sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.9 1 \
    && sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1

ADD requirements.txt /tmp

RUN python -m pip install -r /tmp/requirements.txt

ADD https://github.com/wrgl/wrgl/releases/download/v0.12.7/install.sh /tmp/install_wrgl.sh

RUN sudo chmod +x /tmp/install_wrgl.sh \
    && sudo /tmp/install_wrgl.sh \
    && sudo rm /tmp/install_wrgl.sh /tmp/requirements.txt

ADD https://sdk.cloud.google.com /tmp/install_gcloud.sh
RUN sudo chmod +x /tmp/install_gcloud.sh \
    && sudo /tmp/install_gcloud.sh --install-dir=/usr/local/gcloud
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin
