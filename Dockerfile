FROM public.ecr.aws/glue/aws-glue-libs:5

# Install AWS CLI v2 (latest stable)
RUN yum update -y && \
    yum install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    yum clean all
