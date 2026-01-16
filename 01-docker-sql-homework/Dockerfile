# Use Python 3.13 official image
FROM python:3.13

# Set a working directory inside the container
WORKDIR /home/workspace

# Optional: copy your current folder contents into the container
# COPY . .

# Optional: install common packages for your homework
RUN pip install --no-cache-dir pandas numpy jupyter

# Set bash as the default entrypoint
ENTRYPOINT ["/bin/bash"]