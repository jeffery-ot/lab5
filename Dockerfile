FROM public.ecr.aws/glue/aws-glue-libs:5

# Switch to root to install dependencies
USER root

RUN yum update -y && \
    yum install -y --allowerasing curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    yum clean all

# Switch back to default user
USER glue_user
