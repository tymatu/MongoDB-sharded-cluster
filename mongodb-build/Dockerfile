FROM mongo:6.0.2
COPY /auth/mongodb-keyfile /data
RUN chmod 400 /data/mongodb-keyfile
RUN chown 999:999 /data/mongodb-keyfile

# Install Node.js and npm
RUN apt-get update && apt-get install -y curl
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs