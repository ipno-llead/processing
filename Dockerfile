FROM summerwind/actions-runner:latest

RUN sudo apt update -y \
    && sudo apt remove libgit2-dev \
    && sudo apt install -y software-properties-common \
    build-essential \
    wget \
    cmake \
    libssl-dev \
    pkg-config \
    git \
    && sudo add-apt-repository ppa:deadsnakes/ppa \
    && sudo add-apt-repository ppa:alex-p/tesseract-ocr-devel \
    && sudo apt install -y poppler-utils tesseract-ocr python3.9 python3.9-dev \
    && sudo rm -rf /var/lib/apt/lists/*

ARG libgit2_version=1.5.0

# install libgit2
RUN cd /tmp \
    && git clone --depth=1 -b libssh2-1.9.0 https://github.com/libssh2/libssh2.git /tmp/libssh2_src \
    && cd /tmp/libssh2_src \
    && sudo cmake . -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON -DCMAKE_INSTALL_PREFIX=/tmp/libssh2 \
    && sudo cmake --build . --target install \
    && sudo cp -r /tmp/libssh2/* /usr/bin \
    && sudo cp -r /tmp/libssh2/* /usr/local \
    && sudo ldconfig \
    && git clone --depth=1 -b v${libgit2_version} https://github.com/libgit2/libgit2.git /tmp/libgit2_src \
    && cd /tmp/libgit2_src \
    && sudo cmake . -DBUILD_CLAR=OFF -DCMAKE_BUILD_TYPE=Release -DEMBED_SSH_PATH=/tmp/libssh2_src -DCMAKE_INSTALL_PREFIX=/tmp/libgit2 \
    && sudo cmake --build . --target install \
    && sudo cp -r /tmp/libgit2/* /usr/bin \
    && sudo cp -r /tmp/libgit2/* /usr/local \
    && sudo ldconfig \
    && unset LIBGIT2 \
    && sudo rm -rf /tmp/libssh2_src /tmp/libssh2 /tmp/libgit2_src /tmp/libgit2

# install Make 4.3
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

ADD https://github.com/wrgl/wrgl/releases/download/v0.13.2/install.sh /tmp/install_wrgl.sh

RUN sudo chmod +x /tmp/install_wrgl.sh \
    && sudo /tmp/install_wrgl.sh \
    && sudo rm /tmp/install_wrgl.sh /tmp/requirements.txt
