#list of imports needed 
#Imports for ros2 node
import rclpy
from rclpy.node import Node 

#Import for multithread execution
from rclpy.executors import MultiThreadedExecutor

#Import for timer
import time

#Import for odom
from nav_msgs.msg import Odometry

#Import for 
from geometry_msgs.msg import Twist

#Imports for the path planning
from geometry_msgs.msg import PoseStamped
from nav2_simple_commander.robot_navigator import BasicNavigator, TaskResult

#Import for the cost map
from nav_msgs.msg import OccupancyGrid

#Import for JSON bridge
import json

#Import for the kafka bridge
from kafka import KafkaConsumer

#Threading for the bridge
import threading

#Import math
import math

#Import for the task queue
from collections import deque

#Import for reading the yaml files
import yaml

#Imports for docking and undocking
from nav2_msgs.action import DockRobot
from nav2_msgs.action import UndockRobot
from rclpy.action import ActionClient
from action_msgs.msg import GoalStatus

#Import for getting the path to the yaml files
from ament_index_python.packages import get_package_share_directory
import os

#Import for dynamic costmap update
from map_msgs.msg import OccupancyGridUpdate

# Import to create pointclouds
from sensor_msgs.msg import PointCloud2, PointField
import struct

# The construction of the allocator node
class allocator(Node):

    def __init__(self): 
        super().__init__("allocator")

        # Initialization for the robots NAV2 using their namespaces
        self.robots = {
            "robot1": BasicNavigator(namespace="robot1"),
            "robot2": BasicNavigator(namespace="robot2"),
            "robot3": BasicNavigator(namespace="robot3"),
            # "robot4": BasicNavigator(namespace="robot4"),
            # "robot5": BasicNavigator(namespace="robot5"),
            # "robot6": BasicNavigator(namespace="robot6")
        }

        # Initialization for robots docking server using their namespaces
        self.docking_server = {
            "robot1": ActionClient(self, DockRobot, "/robot1/dock_robot"),
            "robot2": ActionClient(self, DockRobot, "/robot2/dock_robot"),
            "robot3": ActionClient(self, DockRobot, "/robot3/dock_robot"),
            # "robot4": ActionClient(self, DockRobot, "/robot4/dock_robot"),
            # "robot5": ActionClient(self, DockRobot, "/robot5/dock_robot"),
            # "robot6": ActionClient(self, DockRobot, "/robot6/dock_robot")
        }

        # Initialization for robots undocking server using their namespaces
        self.undock_server = {
            "robot1": ActionClient(self, UndockRobot, "/robot1/undock_robot"),
            "robot2": ActionClient(self, UndockRobot, "/robot2/undock_robot"),
            "robot3": ActionClient(self, UndockRobot, "/robot3/undock_robot"),
            # "robot4": ActionClient(self, UndockRobot, "/robot4/undock_robot"),
            # "robot5": ActionClient(self, UndockRobot, "/robot5/undock_robot"),
            # "robot6": ActionClient(self, UndockRobot, "/robot6/undock_robot")            
        }

        # Definition of each robot's maximum velocity in the physical environment
        self.max_velocities = {
            "robot1": float(1),
            "robot2": float(2),
            "robot3": float(3),
            # "robot4": float(4),
            # "robot5": float(5),
            # "robot6": float(6)
        }


        # Definition of each robot's capabilities
        self.Compatibilities = {
            "robot1": ["1", "big_boxes", "ground_level"],
            "robot2": ["2", "big_boxes", "ground_level"],
            "robot3": ["1", "big_boxes", "ground_level"],
            # "robot4": ["2", "big_boxes", "ground_level"], 
            # "robot5": ["1", "small_boxes", "ground_level"], 
            # "robot6": ["1", "small_boxes", "ground_level"]  
        }

        # Weights used in the bid calculations
        self.w_time = 1
        self.w_distance = 1
        self.w_turns = 1
        self.w_cost_map = 1

        # Initializes a dictionary for active robots
        self.active_robots = {}
        
        # Initializes a queue for the tasks
        self.task_queue = deque()

        # Initializes a dictionaries for each active robot
        self.robot_tasks = {}
        self.docking_tasks = {}
        self.active_docking_goals = {}
        self.busy_robots = set()

        # Initializes a dictionary for machines which are turned on
        self.active_machine_zones = {}

        # Initialization of the costmap  
        self.latest_costmap = None


        # Checks whether nav2 is active for each robot
        for robot_id, nav in self.robots.items():
            nav2_active = False
            start_time = time.time() 

            # Timeout if nav2 does not become active within 10 seconds
            while time.time() - start_time < 10: 
                try: 
                    # Waits untill nav2 becomes active
                    nav.waitUntilNav2Active()
                    nav2_active = True
                    break
                except Exception:
                    pass 
            
            # Warns the user if the nav2 instance did not start correctly
            if not nav2_active:
                self.get_logger().warn(f" skipping robot (Nav2 not active)")
                continue
            
            # Stores each active robot in a seperate dictionary
            self.active_robots[robot_id] = nav                   

        # Timers used to call the control loops periodically
        self.create_timer(0.5, self.control_loop)
        self.create_timer(0.1, self.plan_executor)
        
        # Initialising variables used for process checks
        self.request_path = False
        self.have_odom = False

        # Storage for the latets odom and cmd_vel data of each robot
        self.odom = {
            "robot1": None,
            "robot2": None,
            "robot3": None,
            # "robot4": None,
            # "robot5": None,
            # "robot6": None
        }

        self.cmd_vel = {
            "robot1": None,
            "robot2": None,
            "robot3": None,
            # "robot4": None,
            # "robot5": None,
            # "robot6": None            
        }

        # Odom subscriptions for each robot
        # Robot 1
        self.create_subscription(
            Odometry,
            '/robot1/odom',
            lambda msg: self.odom_callback(msg, "robot1"),
            10)
        # Robot 2
        self.create_subscription(
            Odometry,
            '/robot2/odom',
            lambda msg: self.odom_callback(msg, "robot2"),
            10)
        # Robot 3
        self.create_subscription(
            Odometry,
            '/robot3/odom',
            lambda msg: self.odom_callback(msg, "robot3"),
            10)
        # # robot 4
        # self.create_subscription(
        #     Odometry,
        #     '/robot4/odom',
        #     lambda msg: self.odom_callback(msg, "robot4"),
        #     10)
        # # robot 5
        # self.create_subscription(
        #     Odometry,
        #     '/robot5/odom',
        #     lambda msg: self.odom_callback(msg, "robot5"),
        #     10)
        # # robot 6
        # self.create_subscription(
        #     Odometry,
        #     '/robot6/odom',
        #     lambda msg: self.odom_callback(msg, "robot6"),
        #     10)
        
        # cmd_vel subscriptions for each robot
        # Robot 1
        self.create_subscription(
            Twist,
            '/robot1/cmd_vel',
            lambda msg: self.cmd_vel_callback(msg, "robot1"),
            10
            )
        # Robot 2
        self.create_subscription(
            Twist,
            '/robot2/cmd_vel',
            lambda msg: self.cmd_vel_callback(msg, "robot2"),
            10
            )        
        # Robot 3
        self.create_subscription(
            Twist,
            '/robot3/cmd_vel',
            lambda msg: self.cmd_vel_callback(msg, "robot3"),
            10
            )
        # # Robot 4
        # self.create_subscription(
        #     Twist,
        #     '/robot4/cmd_vel',
        #     lambda msg: self.cmd_vel_callback(msg, "robot4"),
        #     10
        #     )
        # # Robot 5
        # self.create_subscription(
        #     Twist,
        #     '/robot5/cmd_vel',
        #     lambda msg: self.cmd_vel_callback(msg, "robot5"),
        #     10
        #     )
        # # Robot 6
        # self.create_subscription(
        #     Twist,
        #     '/robot6/cmd_vel',
        #     lambda msg: self.cmd_vel_callback(msg, "robot6"),
        #     10
        #     )

        # Creates a seperate thread to run the kafka bridge in parallel with the main ros2 node
        self.kafka_thread = threading.Thread(
            target = self.bridge,
            daemon = True
        )
        # Starts the created kafka thread
        self.kafka_thread.start() 

        # Kafka thread for machine status (on/off)
        self.kafka_machine_thread = threading.Thread(
            target = self.bridge_machine,
            daemon=True
        )
        self.kafka_machine_thread.start()

        # Kafka thread for button press
        self.kafka_button_thread = threading.Thread(
            target = self.bridge_button,
            daemon=True
        )
        self.kafka_button_thread.start()

        # Creates a subscription to the global costmap
        self.create_subscription(
            OccupancyGrid,
            "/robot1/global_costmap/costmap",
            self.costmap_callback,
            10
        )

        # Creates a publisher for the pointcloud used in dynamic machine zones
        self.machine_cloud_publisher = {
            "robot1": self.create_publisher(PointCloud2, "/robot1/machine_cloud", 10),
            "robot2": self.create_publisher(PointCloud2, "/robot2/machine_cloud", 10),
            "robot3": self.create_publisher(PointCloud2, "/robot3/machine_cloud", 10),
            # "robot4": self.create_publisher(PointCloud2, "/robot4/machine_cloud", 10),
            # "robot5": self.create_publisher(PointCloud2, "/robot5/machine_cloud", 10),
            # "robot6": self.create_publisher(PointCloud2, "/robot6/machine_cloud", 10)
        }
        self.create_timer(0.1, self.publish_machine_zones)

       
    #The kafka ros2 bridge and the queue
    # Only supports JSON messages and can be extented to include more    
    def bridge(self):
        consumer = KafkaConsumer(
        "tasks", # Topic to which it subscribes
        bootstrap_servers="localhost:9092",  # The kafka server ID
        value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        )

        # The same consumer but connected to the university server
        # consumer = KafkaConsumer(
        # "lngv_tasks", # Topic to which it subscribes
        # bootstrap_servers="redpanda1.et.utwente.nl:9092",  # The kafka server ID
        # value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        # )        
        
        # Creates the directory towards the package containing the dock locations
        package_share = get_package_share_directory('multiple_robots')
        docks_yaml_directory = os.path.join(
            package_share,
            'config',
            'dock_locations.yaml'
        )

        # Reads the dock file
        with open(docks_yaml_directory, "r") as file:
            docks_data = yaml.load(file, Loader=yaml.SafeLoader)
            
        # Gets the dock position and the requirements
        for msg in consumer:
            data = msg.value
            self.dock_name = data["dock"]
            dock_info = docks_data["docks"][self.dock_name]
            pose = dock_info["pose"]
            dock_type = dock_info["type"]
            orientation = pose[2]
            x = pose[0] - 1*math.cos(orientation)
            y = pose[1] - 1*math.sin(orientation)
            compatibility = dock_info["compatibility"]
            end_goal = data["end_dock"]
            task = {
                 "task_id": data["task_id"],
                 "dock_name": data["dock"],
                 "dock_type": dock_type,
                 "x": x,
                 "y": y,
                 "orientation": orientation,
                 "compatibility": compatibility,
                 "attempted_robots": [],
                 "amount_attempts_same_robot": {},
                 "end_dock": end_goal
             }
            
            # Adds the requested task to the queue (on the right)
            self.task_queue.append(task)

    # Kafka bridge for button presses
    def bridge_button(self):
        consumer = KafkaConsumer(
        "buttons", # Topic to which it subscribes
        bootstrap_servers="localhost:9092",  # The kafka server ID
        value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        )

        # The same consumer but connected to the university server
        # consumer = KafkaConsumer(
        # "lngv_buttons", # Topic to which it subscribes
        # bootstrap_servers="redpanda1.et.utwente.nl:9092",  # The kafka server ID
        # value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        # )         

        # Creates the directory towards the file containing the button information (load dock and unloading dock)
        package_share = get_package_share_directory('multiple_robots')
        docks_yaml_directory = os.path.join(
            package_share,
            'config',
            'Buttons_location.yaml'
        )

        # Reads the file
        with open(docks_yaml_directory, "r") as file:
            buttons_data = yaml.load(file, Loader=yaml.SafeLoader)

        # Creates directory for dock locations
        docks_yaml_directory = os.path.join(
            package_share,
            'config',
            'dock_locations.yaml'
        )
        # Reads the file
        with open(docks_yaml_directory, "r") as file:
            docks_data = yaml.load(file, Loader=yaml.SafeLoader)
                
            
        # Gets the dock position, the requirements and which loading and unloading dock
        for msg in consumer:
            data = msg.value
            self.button_number = data["button_number"]
            button_info = buttons_data["buttons"][self.button_number]
            task_id = button_info["task_id"]
            self.button_name = button_info["dock"]
            end_goal = button_info["end_dock"] #unloading dock

            self.dock_name = button_info["dock"] # loading dock
            dock_info = docks_data["docks"][self.dock_name]
            pose = dock_info["pose"]
            dock_type = dock_info["type"]
            orientation = pose[2]
            x = pose[0] - 1*math.cos(orientation)
            y = pose[1] - 1*math.sin(orientation)
            
            compatibility = dock_info["compatibility"]
            
            task = {
                 "task_id": task_id,
                 "dock_name": self.dock_name,
                 "dock_type": dock_type,
                 "x": x,
                 "y": y,
                 "orientation": orientation,
                 "compatibility": compatibility,
                 "attempted_robots": [],
                 "amount_attempts_same_robot": {},
                 "end_dock": end_goal
             }
            
            # Adds the task to the queue
            self.task_queue.append(task)        

            
    # machine status bridge
    def bridge_machine(self):
        consumer = KafkaConsumer(
        "machine_status", # Topic to which it subscribes
        bootstrap_servers="localhost:9092",  # The kafka server ID
        value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        )

        # same bridge but connected to the university server
        # consumer = KafkaConsumer(
        # "lngv_machine_status", # Topic to which it subscribes
        # bootstrap_servers="redpanda1.et.utwente.nl:9092",  # The kafka server ID
        # value_deserializer=lambda m: json.loads(m.decode("utf-8")), #JSON message decoding
        # )        
        
        # Creates the directory towards the file containing the machine locations
        package_share = get_package_share_directory('multiple_robots')
        machine_yaml_directory = os.path.join(
            package_share,
            'config',
            'machine_locations.yaml' # needs to be added
        )

        # Reads the file
        with open(machine_yaml_directory, "r") as file:
            machine_data = yaml.load(file, Loader=yaml.SafeLoader)
            
        # Gets the machine status and the locations
        for msg in consumer:
            data = msg.value
            machine_name = data["machine"]
            machine_info = machine_data["machines"][machine_name]
            pose = machine_info["pose"]
            x = pose[0]
            y = pose[1]
            orientation = pose[2]
            radius = machine_info["radius"]
            self.get_logger().info(f"{machine_name} {data["status"]}")
            machine = {
                 "machine": data["machine"],
                 "status": data["status"],
                 "x": x,
                 "y": y,
                 "orientation": orientation,
             }
            
            # Adds or deletes the machine from the list based on their status
            if machine["status"] == "on":
                self.active_machine_zones[machine_name] = {
                    "x": x,
                    "y": y,
                    "radius": radius,
                    "status": data["status"]
                }
            if machine["status"] == "off":
                self.active_machine_zones.pop(machine_name, None)

    # Definition for creating pointclouds around turned on machines
    def publish_machine_zones(self):
        # amount of points for a full circle and a storage
        num_points = 360
        points = []
        
        # Creation of the circles around each active machine
        for zone in self.active_machine_zones.values():
            for i in range(num_points):
                angle = 2*math.pi * i / num_points
                px = float(zone["x"]+zone["radius"]*math.cos(angle))
                py = float(zone["y"]+ zone["radius"]*math.sin(angle))
                pz = float(0.5)
                points.append((px,py,pz, 100))

        # Creates the message to publish the pointcloud
        cloud = PointCloud2()
        cloud.header.frame_id = "map"
        cloud.header.stamp = self.get_clock().now().to_msg()
        cloud.height = 1
        cloud.width = len(points)
        cloud.is_dense = True
        cloud.is_bigendian = False
        cloud.point_step = 12
        cloud.row_step = cloud.point_step * cloud.width
        cloud.fields = [
            PointField(name = 'x', offset=0, datatype=PointField.FLOAT32, count=1),
            PointField(name = 'y', offset =4, datatype=PointField.FLOAT32, count=1),
            PointField(name = 'z', offset= 8, datatype=PointField.FLOAT32, count=1),
        ]
        cloud.data = b''.join(struct.pack('fff', x, y, z) for x,y,z,_ in points)

        # publishes the created pointcloud
        for pub in self.machine_cloud_publisher.values():
            pub.publish(cloud)

    # Callback to process the received cmd_vel
    def cmd_vel_callback(self, msg, robot_id):
        self.cmd_vel[robot_id] = msg

        # threshold for when the robot is conconsidered moving
        threshold = 0.01
        moving = (
        abs(msg.linear.x) > threshold or
        abs(msg.linear.y) > threshold or
        abs(msg.linear.z) > threshold or
        abs(msg.angular.x) > threshold or
        abs(msg.angular.y)> threshold or
        abs(msg.angular.z) > threshold
        )
        if moving:
            self.busy_robots.add(robot_id)
        else:
            self.busy_robots.discard(robot_id)

    # Callback functions
    def odom_callback(self, msg, robot_id):
        # Stores the latest odom data for the robot
        self.odom[robot_id] = msg
        self.have_odom = True

    # Callback to save the global costmap
    def costmap_callback(self, msg_cost_map):
        self.latest_costmap = msg_cost_map


    # Checks if a new task is available and if odom is received 
    # If this is true, path planning is requested
    def control_loop(self):
        if self.task_queue and self.have_odom:
            self.request_path = True   

    # Checks if path planning should be executed
    def plan_executor(self):
        if not self.request_path:
            return
        if not self.task_queue:
            return
        task = self.task_queue.popleft()
        self.request_path = False
        self.plan_and_allocate(task)

    # Simulating path and allocates the task to the best robot
    def plan_and_allocate(self, task):
       # Initialize dictionaries for bids, paths and estimated travel time
        bids = {}
        paths = {}
        estimated_time = {}

        # Sets a limit on how many times the same robot can try a task
        self.max_retries = 3
        
        
        # Loops through all active robot namespaces
        for robot_id, nav in self.active_robots.items():

            # Initializes variables to get the cost per pose from the cost map
            costmap_cost = 0
            global_costmap = self.latest_costmap
            resolution = global_costmap.info.resolution
            origin_x = global_costmap.info.origin.position.x
            origin_y = global_costmap.info.origin.position.y
            width = global_costmap.info.width
            height = global_costmap.info.height
            data = global_costmap.data            

            # Initializes a variable for the amount turned along the route
            total_theta = 0
            
            # Check for the amount of times the same robot got the task
            failures = task["amount_attempts_same_robot"].get(robot_id, 0)
            if failures >= self.max_retries:
                self.get_logger().warn(f"{robot_id} has exeeded failure limit, assumed to be broken")
                continue 

            # Retrieves the correct odom data for the robot
            odom = self.odom[robot_id] 

            # Fail safe if odom data is unavailable or not published
            if odom is None: 
                bids[robot_id] = float("inf")
                continue
            
            # Rejects the robot by assigning infinite cost if incompatible
            if task["compatibility"] != self.Compatibilities[robot_id]: 
                bids[robot_id] = float("inf")
                self.get_logger().info(f"{robot_id} does not meet requirements")
                if robot_id not in task["attempted_robots"]:
                    task["attempted_robots"].append(robot_id)
                continue
            
            # Rejects robots already assigned a task (requests and RViz)
            if robot_id in self.robot_tasks or robot_id in self.busy_robots:
                bids[robot_id] = float("inf")
                self.get_logger().info(f"{robot_id} is currently doing a task")
                continue 
    
            # Defines the robot start pose and the pose needed before docking
            initial_pose = PoseStamped()
            initial_pose.header.frame_id = 'map'
            initial_pose.header.stamp = nav.get_clock().now().to_msg()
            initial_pose.pose.position.x = odom.pose.pose.position.x
            initial_pose.pose.position.y = odom.pose.pose.position.y
            initial_pose.pose.orientation.z = odom.pose.pose.orientation.z
            initial_pose.pose.orientation.w = odom.pose.pose.orientation.w

            goal_pose = PoseStamped()
            goal_pose.header.frame_id = 'map'
            goal_pose.header.stamp = nav.get_clock().now().to_msg()
            goal_pose.pose.position.x = task["x"]
            goal_pose.pose.position.y = task["y"]
            goal_pose.pose.orientation.w = math.cos(task["orientation"]/2)
            goal_pose.pose.orientation.z = math.sin(task["orientation"]/2)


            # Requests a path from the navigation
            path = nav.getPath(initial_pose, goal_pose)
            
            # If no valid path is found the bid is set to infinite
            if path is None:
                bids[robot_id] = float("inf")
                self.get_logger().error("Path planning failed")
                if robot_id not in task["attempted_robots"]:
                    task["attempted_robots"].append(robot_id)
                continue

            # poses contains all intermediate poses along the route
            poses = path.poses
            total_distance = 0

            # Calculates the total distance and turns along the planned path
            for i in range(1, len(poses)):
                # Distance calculation
                dx = poses[i].pose.position.x - poses[i-1].pose.position.x
                dy = poses[i].pose.position.y - poses[i-1].pose.position.y
                total_distance += (dx**2+dy**2)**0.5
                
                # Translating the robot coordinates onto the costmap
                x = poses[i].pose.position.x
                y = poses[i].pose.position.y
                mx = int((x-origin_x)/resolution)
                my = int((y-origin_y)/resolution)
                index = my*width+mx
                cost = data[index]

                # Sets the cost value to 253 if the value is 254 (lethal obstacle) or 255 (unknown)
                if cost > 253:
                    cost = 253

                # Totall value of the costmap along the route
                costmap_cost += cost

                # Loop to get the cumulative turning angle  
                if i > 1:
                    dx = poses[i].pose.position.x - poses[i-1].pose.position.x
                    dy = poses[i].pose.position.y-poses[i-1].pose.position.y
                    theta = math.atan2(dy, dx)            

                    dx_prev = poses[i-1].pose.position.x - poses[i-2].pose.position.x
                    dy_prev = poses[i-1].pose.position.y-poses[i-2].pose.position.y
                    theta_prev = math.atan2(dy_prev, dx_prev)
 
                    total_theta += abs(theta - theta_prev)

            # normalizes the costmap, preventing it from dominating the bid (2 different methods)
            # costmap_cost_norm = costmap_cost/len(poses) # (amount of poses needed to reach the point)
            costmap_cost_norm = costmap_cost/total_distance # (the distance to this point)
            
            # Stores the planned path
            paths[robot_id] = path

            # Estimate travel time based on path length and robot maximum velocity
            estimated_time[robot_id] = total_distance/self.max_velocities[robot_id]

            # Calculates the bids 
            bids[robot_id] = self.w_distance*total_distance + self.w_time*estimated_time[robot_id] + self.w_turns*total_theta + self.w_cost_map*costmap_cost_norm
            
            #Outputs the bids
            self.get_logger().info("The following values are without the weights")
            self.get_logger().info(f"{robot_id} distance: {total_distance:.2f}")
            self.get_logger().info(f"{robot_id} cost map (normalized): {costmap_cost_norm:.2f}")
            self.get_logger().info(f"{robot_id} turns: {total_theta:.2f}")
            self.get_logger().info(f"{robot_id} time: {estimated_time[robot_id]:.2f}")
            self.get_logger().info(f"{robot_id} bid: {bids[robot_id]:.2f}")

        # Checks if there are no bids
        if not bids:
            self.get_logger().error("no robots available")
            self.task_queue.appendleft(task)
            self.request_path = True
            return

        # Selects robot with minimum bid
        best_robot = min(bids, key=bids.get)
        self.robot_tasks[best_robot] = task

        # Checks if the chosen robot is available
        if bids[best_robot] == float("inf"):
            self.get_logger().info("No robot availabable, waiting")
            self.task_queue.appendleft(task)
            return
        
        dock_name = task["dock_name"]
        self.get_logger().info(f"Selected: {best_robot}")
        self.execute_task(best_robot, task, goal_pose, dock_name)

    # The best robot Executes the task
    def execute_task(self, robot_id, task, goal_pose, dock_name):
        docking_client = self.docking_server[robot_id]
        self.get_logger().info("Waiting for 'DockRobot' action server")
        while not docking_client.wait_for_server(timeout_sec=1.0):
            self.get_logger().info("'DockRobot' action server not available, waiting...")

        # sets up dock request message
        goal_msg = DockRobot.Goal()
        goal_msg.use_dock_id = True
        goal_msg.dock_id = task["dock_name"]
        goal_msg.dock_type = task["dock_type"]

        # send dock message to the docking client
        future = docking_client.send_goal_async(goal_msg)

         # Calls docking result callback when the dock request is accepted or rejected
        future.add_done_callback(
            lambda future: self.docking_response_callback(future, robot_id, task)
        )
        self.get_logger().info(f"docking request sent to {robot_id}")

    # Callback to receive feedback of the docking process
    def docking_response_callback(self, future, robot_id, task):
        if robot_id not in self.robot_tasks:
            self.get_logger().error(f"{robot_id} was not assigned a task")
            return
    
        # Check if the docking request was received by the AGV
        nav = self.robots[robot_id]
        goal_handle = future.result()
        if goal_handle is None:
            self.get_logger().error("Goal handle does not exist")

            # Reallocate when the task is not received
            failed_task = self.robot_tasks[robot_id]
            self.task_queue.appendleft(failed_task)
            if robot_id not in task["amount_attempts_same_robot"]:
                task["amount_attempts_same_robot"][robot_id] = 0
            task["amount_attempts_same_robot"][robot_id] += 1

            # Deletes the robot from the robots currently doing a task
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            time.sleep(1.0)
            self.request_path = True            
            return            

        # Check if the docking request was accepted by the AGV
        if not goal_handle.accepted:
            self.get_logger().info(f"{robot_id} docking not accepted")

            # Reallocate if the task is not accepted
            failed_task = self.robot_tasks[robot_id]
            self.task_queue.appendleft(failed_task)
            if robot_id not in task["amount_attempts_same_robot"]:
                task["amount_attempts_same_robot"][robot_id] = 0
            task["amount_attempts_same_robot"][robot_id] += 1

            # Deletes the robot from the robots currently doing a task
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            self.request_path = True
            return
        
        self.get_logger().info(f"{robot_id} docking accepted")

        # Saves the active docking goal
        self.active_docking_goals[robot_id] = goal_handle

        # Calls docking result callback when the dock is completed (succesfully or unsuccesfully)
        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(
            lambda future: self.docking_result_callback(future, robot_id, task)
        )

    # Callback to check the feedback of the docking process
    def docking_result_callback(self, future, robot_id, task):
        result = future.result()
        status = result.status

        # If docking succeeded the AGV undocks
        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info(f"{robot_id} docked succesfully")
            self.undock_robot(robot_id, task)
            return
        
        # If it did not succeeed the task is canceled and reallocated
        else:
            goal_handle = self.active_docking_goals.get(robot_id)
            if goal_handle is not None:
                cancel_future = goal_handle.cancel_goal_async()
                cancel_future.add_done_callback(
                    lambda f: self.get_logger().info(
                        f"{robot_id} cancel response {f.result().goals_canceling}"
                    )
                )

                # Reallocates the task
                if robot_id in self.robot_tasks:
                    failed_task = self.robot_tasks[robot_id]

                    if robot_id not in failed_task["amount_attempts_same_robot"]:
                        failed_task["amount_attempts_same_robot"][robot_id] = 0
                    failed_task["amount_attempts_same_robot"][robot_id] += 1
                    attempts = failed_task["amount_attempts_same_robot"][robot_id]
                    if attempts+1 > self.max_retries:

                    # Sends undock request to make space for other robots
                        self.task_queue.appendleft(failed_task)
                        self.request_path = True
                        self.undock_robot_extra(robot_id, task)
                        return

                self.task_queue.appendleft(failed_task)
                if robot_id in self.robot_tasks:
                    del self.robot_tasks[robot_id]

                self.request_path = True

    # Function to undock the robot
    def undock_robot(self, robot_id, task):
        # Sets up undock request
        undock_goal = UndockRobot.Goal()
        undock_goal.dock_type = task["dock_type"]
        undocking_client = self.undock_server[robot_id]
        undocking_client.wait_for_server()

        # sends undock request
        future_undock = undocking_client.send_goal_async(undock_goal, feedback_callback=self.undock_feedback_callback)

        # Calls docking result callback when the undock request is accepted or rejected
        future_undock.add_done_callback(
            lambda future_undock: self.undock_response_callback(future_undock, robot_id, task)
        )

    # Function to check undock request
    def undock_response_callback(self, future_undock, robot_id, task):
        goal_handle_undock = future_undock.result()

        # Check if undock request was accepted or not (task is not reallocated)
        if not goal_handle_undock.accepted:
            self.get_logger().info(f"{robot_id} did not accept undock")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            return
        self.get_logger().info(f"{robot_id} undocking")

        # Calls docking result callback when the undock is completed (succesfully or unsuccesfully) 
        result_future_undock = goal_handle_undock.get_result_async()
        result_future_undock.add_done_callback(
            lambda future: self.undock_result_callback(future,robot_id, task)
        )

    # callback to check the undock process
    def undock_result_callback(self, future, robot_id, task):
        result = future.result().result
        status = future.result().status

        #  If it succeeded it goes to it's unloading dock
        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info(f"{robot_id} sending request for unloading dock")
            self.go_to_end_dock(robot_id, task)
        
        # If it fails the task is removed
        else:
            self.get_logger().info(f"{robot_id} undock failed")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]

    # callback function to receive the undock feedback
    def undock_feedback_callback(self, feedback_msg):
        self.get_logger().info(f"Received feedback: {feedback_msg.feedback}")
    
    # Function to send AGV to the unload dock
    def go_to_end_dock(self, robot_id, task):
        end_docking_client = self.docking_server[robot_id]
        while not end_docking_client.wait_for_server(timeout_sec=1.0):
            self.get_logger().info("'DockRobot' action server not available, waiting...")

        # Sets up docking message
        end_goal_msg = DockRobot.Goal()
        end_goal_msg.use_dock_id = True
        end_goal_msg.dock_id = task["end_dock"]

        # Sends dock request
        future = end_docking_client.send_goal_async(end_goal_msg)

        # Calls docking result callback when the dock request is accepted or rejected
        future.add_done_callback(
            lambda future: self.docking_response_callback_end(future, robot_id, task)
        )
        self.get_logger().info(f"{robot_id} going to end dock")

    # Check if the docking request was accapted by the AGV
    def docking_response_callback_end(self, future, robot_id, task):
        # Checks for the right robot_id
        if robot_id not in self.robot_tasks:
            self.get_logger().error(f"{robot_id} was not assigned a task")
            return
        goal_handle_end = future.result()

        # When the dock request is not accepted the task is removed
        if not goal_handle_end.accepted:
            self.get_logger().info(f"{robot_id} docking not accepted")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            return
        
        self.get_logger().info(f"{robot_id} docking accepted")
        self.active_docking_goals[robot_id] = goal_handle_end

        # Calls docking result callback when the dock is completed (succesfully or unsuccesfully)
        result_future = goal_handle_end.get_result_async()
        result_future.add_done_callback(
            lambda future: self.docking_result_callback_end(future, robot_id, task)
        )

    # callback to check the dock process
    def docking_result_callback_end(self, future, robot_id, task):
        result_end = future.result()
        status_end = result_end.status

        # If robot docked it calls the undock
        if status_end == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info(f"{robot_id} docked succesfully")
            self.undock_robot_end(robot_id, task)
            return
        # If it failed it cancels the task
        else:
            goal_handle = self.active_docking_goals.get(robot_id)
            if goal_handle is not None:
                cancel_future = goal_handle.cancel_goal_async()
                cancel_future.add_done_callback(
                    lambda f: self.get_logger().info(
                        f"{robot_id} cancel response"
                    )
                )
                # Sends undock request to make space for other robots
                self.undock_robot_extra(robot_id, task)                
        # removes the task        
        if robot_id in self.robot_tasks:
            del self.robot_tasks[robot_id]
        return

    # Function to undock the robot at the end
    def undock_robot_end(self, robot_id, task):
        # sets up the message to undock
        undock_goal = UndockRobot.Goal()
        undock_goal.dock_type = task["dock_type"]
        undocking_client = self.undock_server[robot_id]
        undocking_client.wait_for_server()

        # sends undock request
        future_undock = undocking_client.send_goal_async(undock_goal, feedback_callback=self.undock_feedback_callback_end)

        # Calls docking result callback when the undock request is accepted or rejected
        future_undock.add_done_callback(
            lambda future_undock: self.undock_response_callback_end(future_undock, robot_id, task)
        )

    # Function to check undock request
    def undock_response_callback_end(self, future_undock, robot_id, task):
        goal_handle_undock = future_undock.result()
        # removes task when it is not accepted
        if not goal_handle_undock.accepted:
            self.get_logger().info(f"{robot_id} did not accept undock")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            return
        
        # Calls docking result callback when the dock is completed (succesfully or unsuccesfully) 
        self.get_logger().info(f"{robot_id} undocking")
        result_future_undock = goal_handle_undock.get_result_async()
        result_future_undock.add_done_callback(
            lambda future: self.undock_result_callback_end(future,robot_id, task)
        )

    # Function to check undock feedback
    def undock_result_callback_end(self, future, robot_id, task):
        result = future.result().result
        status = future.result().status

        # If it succeeded it undocks another time to make more space for other robots
        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info(f"{robot_id} finished task, making space for other robots")
            self.undock_robot_extra(robot_id, task)
        else:
            self.get_logger().info(f"{robot_id} failed to undock at goal dock")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]

    # Function to receive undock feedback
    def undock_feedback_callback_end(self, feedback_msg):
        self.get_logger().info(f"Received feedback: {feedback_msg.feedback}")

    # Function to undock robots when a docking, undock failed or succeeded
    def undock_robot_extra(self, robot_id, task):
        # Sets uo undock message 
        undock_goal_extra = UndockRobot.Goal()
        undock_goal_extra.dock_type = task["dock_type"]
        undocking_client = self.undock_server[robot_id]
        undocking_client.wait_for_server()

        # Sends undock message
        future_undock = undocking_client.send_goal_async(undock_goal_extra, feedback_callback=self.undock_feedback_callback_extra)

        # Calls undocking result callback when the dock is completed (succesfully or unsuccesfully)
        future_undock.add_done_callback(
            lambda future_undock: self.undock_response_callback_extra(future_undock, robot_id, task)
        )

    # Function to check if undock was accepted
    def undock_response_callback_extra(self, future_undock, robot_id, task):
        goal_handle_undock = future_undock.result()

        # If it was not accepted the task is removed
        if not goal_handle_undock.accepted:
            self.get_logger().info(f"{robot_id} did not accept undock")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            return
        self.get_logger().info(f"{robot_id} undocking")

        # Calls undocking result callback when the undock is completed (succesfully or unsuccesfully) 
        result_future_undock = goal_handle_undock.get_result_async()
        result_future_undock.add_done_callback(
            lambda future: self.undock_result_callback_extra(future,robot_id, task)
        )

    #Function to check undock process
    def undock_result_callback_extra(self, future, robot_id, task):
        result = future.result().result
        status = future.result().status

        if status == GoalStatus.STATUS_SUCCEEDED:
            self.get_logger().info(f"{robot_id} made space for other robots")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]
            return
        else:
            self.get_logger().info(f"{robot_id} failed to undock to make space for other robots")
            if robot_id in self.robot_tasks:
                del self.robot_tasks[robot_id]

    def undock_feedback_callback_extra(self, feedback_msg):
        self.get_logger().info(f"Received feedback: {feedback_msg.feedback}")                   

def main(args=None):
    rclpy.init(args=args)

    # Initialize the allocator node
    allocator_on = allocator()

    # create multi thread executor 
    executor = MultiThreadedExecutor()

    # Register allocator node to the executor
    executor.add_node(allocator_on)
 
    # Start ros 2 event loop
    try:
        executor.spin()
    # Destroys the node if it is stopped with ctrl c
    finally:
        allocator_on.destroy_node()
        for nav in allocator_on.robots.values():
            nav.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()


    