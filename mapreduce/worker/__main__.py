"""MapReduce framework Worker node."""
import os
import logging
import json
import time
import threading
import socket
import hashlib
import subprocess
import tempfile
import heapq
from contextlib import ExitStack
import shutil
import click
from mapreduce.utils.tcp import connect

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a Worker node in a MapReduce cluster."""

    def __init__(self, host, port, manager_host, manager_port):
        """Construct a Worker instance and start listening for messages."""
        self.signal = {"shutdown": False}
        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s",
            manager_host, manager_port,
        )

        message_dict = {
            "message_type": "register",
            "worker_host": host,
            "worker_port": port,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # tcp - listen for incoming messages
        mess = threading.Thread(target=self.tcp_server,
                                args=(host, port, manager_host, manager_port))
        mess.start()
        # self.tcp_server(host, port, manager_host, manager_port)

        # registration
        # send register message to Manager using host/port
        self.tcp_client(message_dict, manager_host, manager_port)

        # self.signal['shutdown'] = True  #?
        mess.join()
        # while not self.signal['shutdown']:
        #     if self.signal['shutdown'] == True:
        #         messages.join()
        #         break

    def tcp_server(self, host, port, manager_host, manager_port):
        """TCP Socket Server."""
        # Create an INET, STREAMing socket, this is TCP
        # Note: context manager syntax allows for sockets to automatically be
        # closed when an exception is raised or control flow returns.

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
            sock.listen()
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
            hear = threading.Thread(target=self.udp_client,
                                    args=(host, port,
                                          manager_host, manager_port))
            while True:
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                LOGGER.info("Connection from %s", address[0])

                clientsocket.settimeout(1)
                message_str = connect(socket, clientsocket)

                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue

                # manager sends register_ack, worker receives it
                if message_dict['message_type'] == 'register_ack':
                    # worker receives a register_ack
                    LOGGER.info("Received register ack")
                    hear.start()
                # manager sends new task, worker receives it
                elif message_dict['message_type'] == 'new_map_task':
                    LOGGER.info("Map task")
                    # handle mapping
                    # run map executable
                    self.map_task(message_dict, manager_host, manager_port)
        # https://stackoverflow.com/questions/56120633/how-to-sort-a-text-file-line-by-line
                elif message_dict['message_type'] == 'new_reduce_task':
                    LOGGER.info("Reduce task")
                    # Merge input files into one sorted output stream.
                    self.reduce_task(message_dict, manager_host, manager_port)

                # manager gets shutdown request, forwards to workers
                elif message_dict['message_type'] == 'shutdown':
                    self.signal['shutdown'] = True
                    hear.join()
                    break

    def tcp_client(self, message_dict, manager_host, manager_port):
        """Test TCP Socket Client."""
        # create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((manager_host, manager_port))
            # send a message
            message = json.dumps(message_dict)
            sock.sendall(message.encode('utf-8'))
            # Worker sends register message to manager
            # Ex. Manager sends a task to the worker
            # Ex. Worker finishes the task and sends back
            # Manager listens for finished tasks from the worker
            # Worker listens for incoming tasks from the manager

    def udp_client(self, host, port, manager_host, manager_port):
        """Udp client."""
        # Create an INET, DGRAM socket, this is UDP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # Connect to the UDP socket on server
            sock.connect((manager_host, manager_port))
            # Send a message
            message = json.dumps(
                    {
                        'message_type': 'heartbeat',
                        'worker_host': host,  # or worker's host and port?
                        'worker_port': port
                    }
                )
            LOGGER.info(self.signal["shutdown"])
            # while not shutdown
            while not self.signal['shutdown']:
                sock.sendall(message.encode('utf-8'))
                time.sleep(2)
                # sock.settimeout(2)
                if self.signal['shutdown'] is True:
                    break
            # worker sending heartbeat every 2 seconds

    def map_task(self, message_dict, manager_host, manager_port):
        """Map task."""
        tid = message_dict["task_id"]
        prefix = f"mapreduce-local-task{f'{tid:05d}'}-"

        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with ExitStack() as stack:
                # self.run_executable(tmpdir, message_dict)
                # open input_files ahead of time
                input_files = message_dict["input_paths"]
                context_files = []
                for file in input_files:  # loop through input_files
                    context_files.append(
                        stack.enter_context(open(file, encoding="utf-8")))
                    # store and open input files in context_files

                temp_files = []
                # create and open intermediate partition files
                for part in range(message_dict['num_partitions']):
                    part_file = f"maptask{f'{tid:05d}'}-part{f'{part:05d}'}"
                    temp_files.append(
                        stack.enter_context(
                            open(
                                os.path.join(tmpdir, part_file),
                                "a+", encoding="utf-8"
                                )
                        )
                    )

                self.inter(context_files,
                           temp_files,
                           message_dict['num_partitions'],
                           message_dict["executable"])
            # Sort each output file by line.
            self.move_sorted_output(tmpdir, message_dict["output_directory"])
        LOGGER.info("sending finish messagge for map")
        # Once a Worker has finished its task
        finished_mess = {
            "message_type": "finished",
            "task_id": message_dict["task_id"],
            "worker_host": message_dict["worker_host"],
            "worker_port": message_dict["worker_port"]
        }
        self.tcp_client(finished_mess, manager_host, manager_port)

    def move_sorted_output(self, tmpdir, output_dir):
        """Move sorted output."""
        for fname in os.listdir(tmpdir):
            joined = os.path.join(tmpdir, fname)
            with open(joined, 'r+', encoding="utf-8") as part:
                lines = part.readlines()
                lines.sort()
                part.seek(0)  # remove top
                # f.truncate()
                part.writelines(lines)
            LOGGER.info("tmpdir/fname %s", os.path.join(tmpdir, fname))
            # Move the sorted output files
            shutil.move(os.path.join(tmpdir, fname),
                        os.path.join(output_dir))

    def inter(self, context_files, temp_files, num_partitions, executable):
        """Write to intermediate files."""
        for infile in context_files:
            with subprocess.Popen(
                [executable],
                stdin=infile,
                stdout=subprocess.PIPE,
                text=True,
            ) as map_process:
                for line in map_process.stdout:
                    # make into key, value pair
                    key = line.split("\t")[0]
                    hexdigest = hashlib.md5(
                        key.encode("utf-8")).hexdigest()
                    keyhash = int(hexdigest, base=16)
                    par = keyhash % num_partitions
                    temp_files[par].write(line)

    def run_executable(self, tmpdir, message_dict):
        """Run mapper executable."""
        with ExitStack():
            tid = message_dict["task_id"]
            for input_file in message_dict["input_paths"]:
                with open(input_file, encoding="utf-8") as infile:
                    with subprocess.Popen(
                        [message_dict["executable"]],
                        stdin=infile,
                        stdout=subprocess.PIPE,
                        text=True,
                    ) as map_process:
                        for line in map_process.stdout:
                            # make into key, value pair
                            key = line.split("\t")[0]
                            hexdigest = hashlib.md5(
                                key.encode("utf-8")).hexdigest()
                            keyhash = int(hexdigest, base=16)
                            p_h = keyhash % message_dict["num_partitions"]

                            file = f"maptask{f'{tid:05d}'}-part{f'{p_h:05d}'}"
                            part_out = os.path.join(tmpdir, file)
                            # if os.path.exists(partition_out_path) == False:
                            #     pathlib.Path(partition_out_path).touch()
                            with open(part_out, 'a+', encoding="utf-8") as out:
                                out.write(line)

    def reduce_task(self, message_dict, manager_host, manager_port):
        """Reduce task."""
        pat = message_dict['input_paths']
        prefix = f"mapreduce-local-task{message_dict['task_id']:05d}-"

        with tempfile.TemporaryDirectory(prefix=prefix) as tmpdir:
            with ExitStack() as stack:
                files = []
                # open input files (pat)
                for file in pat:
                    files.append(
                        stack.enter_context(open(file,
                                                 encoding="utf-8")))

                joined = os.path.join(tmpdir,
                                      f"part-{message_dict['task_id']:05d}")
                outfile = stack.enter_context(
                    open(joined, "w+", encoding="utf-8"))
                # with open(joined, 'w', encoding="utf-8") as outfile:
                with subprocess.Popen(
                    [message_dict['executable']],
                    text=True,
                    stdin=subprocess.PIPE,
                    stdout=outfile,
                ) as reduce_process:
                    # Pipe input to reduce_process
                    for line in heapq.merge(*files):
                        reduce_process.stdin.write(line)

                # Move the output file to the final output.
                # os.rename(joined,
                # os.path.join(message_dict['output_directory'],
                # outname))
                shutil.move(joined,
                            os.path.join(message_dict["output_directory"]))
        finished_mess = {
            "message_type": "finished",
            "task_id": message_dict['task_id'],
            "worker_host": message_dict["worker_host"],
            "worker_port": message_dict["worker_port"]
        }
        self.tcp_client(finished_mess, manager_host, manager_port)


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
def main(host, port, manager_host, manager_port, logfile, loglevel):
    """Run Worker."""
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(f"Worker:{port} [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Worker(host, port, manager_host, manager_port)


if __name__ == "__main__":
    main()
