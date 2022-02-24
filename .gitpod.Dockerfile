FROM gitpod/workspace-rust

USER gitpod

RUN sudo apt-get update && \
    sudo apt-get install -y \
        llvm-dev \
        libclang-dev
        clang && \
    sudo rm -rf /var/lib/apt/lists/*
