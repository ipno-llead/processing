FROM summerwind/actions-runner:latest

RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg \
&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null \
&& sudo apt-get update \
&& sudo apt-get install -y docker-ce-cli \
&& sudo rm -rf /var/lib/apt/lists/*

RUN sudo add-apt-repository ppa:deadsnakes/ppa \
&& sudo add-apt-repository ppa:alex-p/tesseract-ocr-devel \
&& sudo apt update -y \
&& sudo apt remove -y libgit2-dev \
&& sudo apt install -y software-properties-common \
build-essential \
wget \
cmake \
libssl-dev \
pkg-config \
git \
poppler-utils \
tesseract-ocr \
python3.10 \
python3.10-dev \
python3.10-distutils \
curl \
libpq-dev \
postgresql-client \
&& sudo rm -rf /var/lib/apt/lists/*

RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10

# Install yq from GitHub releases (PPA doesn't support Ubuntu Noble)
RUN sudo wget -qO /usr/local/bin/yq https://github.com/mikefarah/yq/releases/latest/download/yq_linux_$(dpkg --print-architecture) \
&& sudo chmod +x /usr/local/bin/yq

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
ADD --chown=runner:runner http://ftp.gnu.org/gnu/make/make-4.3.tar.gz /tmp/
RUN cd /tmp \
&& tar -xzf make-4.3.tar.gz \
&& cd make-4.3 \
&& ./configure --prefix=/usr/local \
&& make \
&& sudo make install \
&& rm -rf /tmp/make-4.3 /tmp/make-4.3.tar.gz

RUN sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
&& sudo update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

ADD requirements.txt /tmp

RUN python3.10 -m pip install --break-system-packages 'pip<24.1' setuptools wheel \
&& python3.10 -m pip install --break-system-packages -r /tmp/requirements.txt

# ADD https://github.com/wrgl/wrgl/releases/download/v0.13.7/install.sh /tmp/install_wrgl.sh

# RUN sudo chmod +x /tmp/install_wrgl.sh \
# && sudo /tmp/install_wrgl.sh \
# && sudo rm /tmp/install_wrgl.sh /tmp/requirements.txt

ADD https://sdk.cloud.google.com /tmp/install_gcloud.sh
RUN sudo chmod +x /tmp/install_gcloud.sh \
&& sudo /tmp/install_gcloud.sh --install-dir=/usr/local/gcloud
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin
