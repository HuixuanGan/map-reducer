"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import collections
import threading
import socket
import shutil
from contextlib import ExitStack
import click
from mapreduce.utils.tcp import connect


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.job_id = 0
        self.jobs = collections.deque()  # queue of jobs
        self.tasks = collections.deque()
        self.reducer_tasks = collections.deque()
        self.workers = []  # worker states are ready, busy, or dead
        self.running = False
        self.signal = {"shutdown": False}  # shutdown signal

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        handle_jobs = threading.Thread(target=self.handle_jobs)
        handle_jobs.start()

        # udp - receive heartbeat from worker
        heartbeat = threading.Thread(target=self.udp_server,
                                     args=(host, port))
        heartbeat.start()

        # thread for fault tolerance -
        # fault = threading.Thread(target=, args=(host, port))
        checking = threading.Thread(target=self.checking)
        checking.start()

        self.tcp_server(host, port)
        # # tcp - send messages to worker
        # create tcp server socket to listen for shutdown & other messages
        # listener=threading.Thread(target=self.tcp_server, args=(host, port))
        # listener.start()

        # keep on main thread to keep shutdown from happening too early?
        # self.signal['shutdown'] = True
        # listener.join()
        # heartbeat.join()
        heartbeat.join()
        checking.join()
        handle_jobs.join()

    def tcp_server(self, host, port):
        """TCP Socket Server."""
        print("Inside tcp server in manager")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            print("Created tcp server socket in manager")
            while not self.signal["shutdown"]:
                try:
                    clientsocket1, address1 = sock.accept()
                except socket.timeout:
                    continue
                message_str = connect(socket, clientsocket1)
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(address1)

                # When a registration message is received, send back an ack
                if message_dict['message_type'] == 'register':
                    add = (
                        {"status": "ready",
                         "worker_host": message_dict["worker_host"],
                         "worker_port": message_dict["worker_port"],
                         "time": time.time()})
                    self.workers.append(add)
                    LOGGER.info("Received register message from worker")
                    register_ack = {
                        "message_type": "register_ack",
                        "worker_host": message_dict["worker_host"],
                        "worker_port": message_dict["worker_port"],
                    }
                    success = self.tcp_client(register_ack,
                                              message_dict["worker_host"],
                                              message_dict["worker_port"])
                    if not success:
                        self.mark_as_dead(
                                          message_dict["worker_host"],
                                          message_dict["worker_port"])
                    print("sent register_ack to worker")
                elif message_dict['message_type'] == 'new_manager_job':
                    LOGGER.info("Received new manager job ")
                    message_dict['job_id'] = self.job_id
                    self.job_id += 1
                    output_dir = message_dict['output_directory']
                    # delete output directory if it exists
                    if os.path.exists(output_dir) is True:
                        shutil.rmtree(output_dir)
                        LOGGER.info("---DELETING OUTPUT DIRECTORY---")
                    # create output directory
                    os.makedirs(output_dir)
                    LOGGER.info("Created output directory %s", output_dir)
                    self.jobs.append(message_dict)
                # receive complete_job
                elif message_dict['message_type'] == 'finished':
                    self.finished(message_dict)
                # manager gets shutdown request, forwards to workers
                elif message_dict['message_type'] == 'shutdown':
                    LOGGER.info("!---RECEIVED SHUTDOWN REQUEST---!")
                    for worker in self.workers:
                        if worker["status"] != "dead":
                            # create new TCP client to forward the message
                            success = self.tcp_client(message_dict,
                                                      worker["worker_host"],
                                                      worker["worker_port"])
                            if success:
                                worker["status"] = "dead"
                        # worker.signal["shutdown"] = True
                    self.signal["shutdown"] = True
                    break
        LOGGER.info("Ended tcp server thread.")

    def mark_as_dead(self, host, port):
        """Mark worker as dead."""
        for worker in self.workers:
            if (worker["worker_host"] == host
                    and worker["worker_port"] == port):
                worker["status"] = "dead"
                break

    def tcp_client(self, message_dict, host, port):
        """Test TCP Socket Client."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            try:
                sock.connect((host, port))
                # send a message
                message = json.dumps(message_dict)
                sock.sendall(message.encode('utf-8'))
                print(message)
            except ConnectionRefusedError:
                LOGGER.info("ConnectionRefusedError")
                return False
            return True
            # Worker sends register message to manager
            # Ex. Manager sends a task to the worker
            # Ex. Worker finishes the task and sends back
            # Manager listens for finished tasks from the worker
            # Worker listens for incoming tasks from the manager

    def udp_server(self, host, port):
        """Udp server."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Bind the UDP socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.settimeout(1)
            # No sock.listen() since UDP doesn't establish connections like TCP
            # Receive incoming UDP messages
            while not self.signal['shutdown']:
                try:
                    message_bytes = sock.recv(4096)
                except socket.timeout:
                    continue
                message_str = message_bytes.decode("utf-8")
                message_dict = json.loads(message_str)
                LOGGER.info("receive heartbeat")
                check = time.time()
                for worker in self.workers:
                    if (worker["worker_port"] == message_dict["worker_port"]
                            and
                            worker["worker_host"]
                            == message_dict["worker_host"]):
                        worker['time'] = check
        LOGGER.info("---UDP SERVER ENDING---")

    def checking(self):
        """Check for dead workers: fault tolerance."""
        LOGGER.info("starting fault tolerance")
        while not self.signal['shutdown']:
            time.sleep(0.1)
            check = time.time()
            for worker in self.workers:
                if check - worker["time"] >= 10 and worker["status"] == 'busy':
                    LOGGER.info("Handle overtime")
                    # if worker["status"]=="busy":
                    # if map
                    worker["status"] = 'dead'
                    if worker["message_type"] == "new_map_task":
                        self.tasks.append(worker["task"])
                        LOGGER.info("Added new map task")
                    # if reduce
                    elif worker["message_type"] == "new_reduce_task":
                        self.reducer_tasks.append(worker["task"])

        LOGGER.info("Ended fault tolerance thread.")

    def handle_jobs(self):
        """Handle jobs."""
        LOGGER.info("Handle jobs thread")
        while not self.signal['shutdown']:
            # change while to if?
            if self.jobs and not self.running:
                LOGGER.info("!---RUNNING JOB---!")
                self.running = True
                self.handle_files()
                self.running = False
                LOGGER.info("!---JOB DONE---!")
            # while self.jobs and not self.running:
                # self.running = True
                # self.handle_files()
                # self.running = False
        LOGGER.info("End of handle jobs thread")

    def handle_files(self):
        """Handle files."""
        # Can't handle failed executions of tasks due to being
        c_job = self.jobs.popleft()  # pop from an empty queue?
        current_job_id = c_job["job_id"]
        prefix = f"mapreduce-shared-job{current_job_id:05d}-"
        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with ExitStack():
                LOGGER.info("Created tmpdir %s", tmpdir)
                # open input directory from message_dict["input_directory"]
                filenames = self.sort_for_map(c_job)
                # partitioning
                file_arrs = [[] for i in range(c_job["num_mappers"])]

                for i, filename in enumerate(filenames):
                    file_arrs[i % c_job["num_mappers"]].append(filename)
                    # file_arrs
                    #   0: [1, 5]
                    #   1: [2]
                    #   2: [3]
                    #   3: [4]

                for i, j in enumerate(file_arrs):
                    self.tasks.append({'task': j,
                                       'task_id': i % c_job["num_mappers"]})

                while ((self.tasks or not self.check_stage_complete())
                        and not self.signal["shutdown"]):
                    time.sleep(0.1)
                    for worker in self.workers:
                        if worker["status"] == "ready" and self.tasks:
                            worker["task"] = self.tasks.popleft()
                            worker["message_type"] = "new_map_task"
                            message_dict = {
                                "message_type": "new_map_task",
                                "task_id": worker["task"]["task_id"],
                                "input_paths": worker["task"]["task"],
                                "executable": c_job["mapper_executable"],
                                "output_directory": tmpdir,
                                "num_partitions": c_job["num_reducers"],
                                "worker_host": worker["worker_host"],
                                "worker_port": worker["worker_port"]
                            }

                            success = self.tcp_client(message_dict,
                                                      worker["worker_host"],
                                                      worker["worker_port"])
                            if not success:
                                worker["status"] = "dead"
                                self.tasks.append(worker["task"])
                                break
                            worker["status"] = "busy"
                            LOGGER.info(message_dict)
                            # t_id = (t_id + 1) % c_job["num_mappers"]
                            time.sleep(1)
                            break
                # wait for mapping to finish???????
                LOGGER.info("---LEAVING MAPPING STAGE---")
                filenames = self.sort_for_reduce(tmpdir)
                # reduce when all map files finish
                file_arrs = [[] for i in range(c_job["num_reducers"])]

                for i, filename in enumerate(filenames):
                    t_id = i % c_job["num_reducers"]
                    file_arrs[t_id].append(filename)
                num_r = c_job["num_reducers"]
                for i, j in enumerate(file_arrs):
                    self.reducer_tasks.append({'task': j,
                                               'task_id': i % num_r})
                # reduce_file[0] = partition0 -> task_id = 0
                # reduce_file[1] = partition1 -> task_id = 1

                while ((self.reducer_tasks or not self.check_stage_complete())
                        and not self.signal["shutdown"]):
                    time.sleep(0.1)
                    for worker in self.workers:
                        if worker["status"] == "ready" and self.reducer_tasks:
                            worker["task"] = self.reducer_tasks.popleft()
                            worker["message_type"] = "new_reduce_task"
                            message_dict = {
                                "message_type": "new_reduce_task",
                                "task_id": worker["task"]["task_id"],
                                "input_paths": worker["task"]["task"],
                                "executable": c_job["reducer_executable"],
                                "output_directory": c_job["output_directory"],
                                "worker_host": worker["worker_host"],
                                "worker_port": worker["worker_port"]
                            }
                            LOGGER.info(worker)
                            success = self.tcp_client(message_dict,
                                                      worker["worker_host"],
                                                      worker["worker_port"])
                            if not success:
                                worker["status"] = "dead"
                                self.reducer_tasks.append(worker["task"])
                                break
                            worker["status"] = "busy"
                            LOGGER.info(message_dict)
                            # t_id = (t_id + 1) % c_job["num_reducers"]
                            time.sleep(1)
                            break
                LOGGER.info("---LEAVING REDUCING STAGE---")
            # check the jobs are all finished
            # for worker in self.workers:
            #     if worker["status"]=="busy":
            #         LOGGER.info("job still running ?")

            LOGGER.info("Cleaned up tmpdir %s", tmpdir)
            # self.signal["shutdown"] = True
        LOGGER.info("---LEAVING HANDLE_FILES---")

    def sort_for_map(self, c_job):
        """Sort files for map stage."""
        filenames = []
        for file in os.listdir(c_job["input_directory"]):
            joined = os.path.join(c_job["input_directory"], file)
            filenames.append(joined)
        filenames.sort()
        return filenames

    def sort_for_reduce(self, tmpdir):
        """Sort files in the given directory."""
        filenames = []
        for filename in os.listdir(tmpdir):
            filenames.append(os.path.join(tmpdir, filename))
        filenames.sort()
        return filenames

    def check_stage_complete(self):
        """Check if mapping tasks completed."""
        for worker in self.workers:
            if worker["status"] == "busy":
                return False
        return True

    def finished(self, message_dict):
        """Make worker ready."""
        for worker in self.workers:
            if worker["worker_host"] == message_dict["worker_host"]:
                if worker["worker_port"] == message_dict["worker_port"]:
                    worker["status"] = "ready"
        # increment the number of finished messages
        LOGGER.info("---LEAVING FINISHED---")


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
