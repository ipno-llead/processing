FROM summerwind/actions-runner:latest

RUN sudo apt update -y \
    && sudo apt install -y software-properties-common \
    && sudo add-apt-repository ppa:deadsnakes/ppa \
    && sudo add-apt-repository ppa:alex-p/tesseract-ocr-devel \
    && sudo apt install -y poppler-utils tesseract-ocr python3.9 \
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
