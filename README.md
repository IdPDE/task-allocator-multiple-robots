# ROS2-Task-Allocator
Multipe robots (3 swerve drive and 3 diff drive) in Gazebo and Rviz with Nav2. Every node/topic/controller are linked to a robot by namespace. Including an automatic task allocation system.

The test environment is ROS2 Jazzy, Kafka or Redpanda connected (local or university server) to the task allocator.

## Content
This repository currently includes the following packages:
* allocator

This task allocator repository needs the repository [multiple_robots](https://github.com/IdPDE/multiple-robots.git)

## Installation instructions & dependencies

To install the packages from inside your workspace:
```console
cd src
git clone https://github.com/IdPDE/...
```
Make sure that the following are properly installed in the ROS2 environment: 
* Virtual environment (venv), inside the venv the following python packages need to be installed: Kafka, numpy and yaml
* Outside the ROS2 environment kafka_2.13-4.2.0 needs to be installed, this is needed to create publishers to the task allocator or to use a local server
* multiple-robots repository

## Execution of the project
The folder multiple_robots-Jazzy should be added to a folder which also contains a kafka installation after which the instructions below can be followed to launch the multiple robot environment and the task allocator.

The package for multiple robots can be launched by using the command:

    ros2 launch multiple_robots robots.launch.py

You can start with goal planning if the Nav2 plugin in Rviz2 shows Navigation and Localization as active. Sometimes due to the large number of nodes to be launched, a single robot fails to launch, try to launch again, most of the time this will help.  

The following explains how to launch the allocator.

To send messages to the allocator, the kafka server needs to be started and or connected to. If Kafka is not installed follow this tutorial, https://kafka.apache.org/quickstart/, step 1 and step 2 using downloaded files.

If the university server wants to be used, follow these steps:

To send messages to the allocator publishers need to be created and connected to the following topics. 

topic lngv_tasks

topic lngv_machine_status

topic lngv_buttons 

After creating publishers for these topics the allocator can be launched.


If a local server wants to be used, follow these steps: 

Open another terminal and start a kafka server, the command: for this laptop 

Once the server is started create the following topics (in a new terminal), using these commands:

cd to the kafka folder 
bin/kafka-topics.sh --create --topic tasks --bootstrap-server localhost:9092

cd to the kafka folder 
bin/kafka-topics.sh --create --topic machine_status --bootstrap-server localhost:9092

cd to the kafka folder 
bin/kafka-topics.sh --create --topic buttons --bootstrap-server localhost:9092

Start three different terminals to create the publishers to the created Kafka topics, use the following commands to create the publishers:

cd to the kafka folder
bin/kafka-console-producer.sh --topic tasks --bootstrap-server localhost:9092

cd to the kafka folder
bin/kafka-console-producer.sh --topic machine_status --bootstrap-server localhost:9092

cd to the kafka folder
bin/kafka-console-producer.sh --topic buttons --bootstrap-server localhost:9092

After these are created and connected, the allocator can be launched.

Before starting the allocator, make sure the allocator code (bridges) is set to the correct server (local or university)

This explains how to launch the allocator.

In another terminal, activate the venv (virtual environment), which uses a python 3.12 with the extentions Kafka, numpy and yaml. To activate it, go into the directory (src) just before the folder called venv. After which the following can be used to actiate it: source venv/bin/activate
The terminal should show venv before it shows the current folder you are located in, if this is the case, go back to the directory the ros2 environment is also launched from. From there the following launches the allocator: python3 allocator/allocator/allocator_node.py
If each navigation shows that it is active in the terminal the allocator is ready to use.

Sometimes the venv is not correctly installed from the zip. In this case follow the tutorial (https://docs.ros.org/en/rolling/How-To-Guides/Using-Python-Packages.html) starting from 'Installing via a virtual environment'.
Installing python 3.12 and the following python extentions: Kafka, numpy and yaml.
