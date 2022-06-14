package com.kafkamock;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.core.DockerClientBuilder;

import java.util.List;

public class DockerLocalClient {

    private DockerClient dockerClient;

    public DockerLocalClient() {
        this.dockerClient = DockerClientBuilder.getInstance().build();
    }

    /**
     * Returns Container Lists via dockerClient
     * @return
     */
    public List<Container> getContainerList() {
        return dockerClient.listContainersCmd().exec();
    }

    /**
     * Stops specified container with param containerId
     * @param containerId
     */
    public void stopContainer(String containerId) {
        dockerClient.stopContainerCmd(containerId).exec();
    }

    /**
     * Stops specified container with param containerId
     * @param containerId
     */
    public void startContainer(String containerId) {
        dockerClient.startContainerCmd(containerId).exec();
    }
}
