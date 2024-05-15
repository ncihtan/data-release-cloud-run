FROM --platform=linux/amd64 python:3.11-slim

RUN apt-get update 
RUN apt install curl -y 

ENV PATH=/google-cloud-sdk/bin:$PATH

RUN export CLOUD_SDK_VERSION="410.0.0" && \
    curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    ln -s /lib /lib64

WORKDIR /app

COPY . /app

RUN pip3 install --no-cache-dir --upgrade \
    -r /app/requirements.txt

CMD ["python3","release.py"]
