import zmq
import json
from loguru import logger
import pathlib
import utils


class ControllerLink():
    """Handles the communication with the asf controller.
    Every message send in dict gets acknoledged by the controller.
    
    data that will be sent, should have the following format:

        data = {"panel_orientation": [azimuth_0, altitude_0,
                                      ...
                                      azimuth_30, altitude_30]}
        where the number of the panel corresponds to 
            nr_panel = row*6 + col

    Example Usage:
    controller_link = ControllerLink("192.168.0.10", port="5566")
    status = controller_link.communicate(data)
    if status:
        continue
    else:
        break
    
    """

    def __init__(self, ip, port="5566"):
        self.context = zmq.Context()
        logger.debug(f"Connecting to controller {ip}:{port}")
        self.ip = ip
        self.port = port
        self.req_socket = None
        self._setup_connection()

    def _setup_connection(self):
        """ call sets sockets settings and connects to controller
        """        
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect(f"tcp://{self.ip}:{self.port}")
        self.req_socket.setsockopt(zmq.RCVTIMEO, 50)

    def communicate(self, data):
        """send data to controller and return status

        Args:
            data (dict): every object stored within key in dict

        Returns:
            bool:  message was received sucessfully
        """
        self.req_socket.send_json(json.dumps(data, cls=utils.NpEncoder))
        try:
            reply = self.req_socket.recv()
        except zmq.error.Again as ea:
            reply = ea
            logger.warning("No response from server")
            self._setup_connection()

        if reply != b"ok":
            logger.info(f"not ok - Reply: {reply}")
            return 0

        return 1

    def all_opening():
        open_state =  {"panel_orientation": [ axis for panel in range(30) for axis in [0, 90] ]}
        self.communicate(open_state)
    
    def all_closing():
        closed_state =  {"panel_orientation": [ axis for panel in range(30) for axis in [0, 10] ]}
        self.communicate(closed_state)
    
    def all_east():
        all_east =  {"panel_orientation": [ axis for panel in range(30) for axis in [-45, 45] ]}
        self.communicate(all_east)   

    def all_west():
        all_west =  {"panel_orientation": [ axis for panel in range(30) for axis in [45, 45] ]}
        self.communicate(all_west)
    
    def all_panels(azimuth, altitude):
        all_panels =  {"panel_orientation": [ axis for panel in range(30) for axis in [azimuth, altitude] ]}
        self.communicate(all_panels)
        

class Datalogger:
    """ This a a zmq subscription datalogger. Takes everything and writes it to file.
    Can record by calling:
        datalogger = zmqhandler.ZmqDataLogger(ip=...,
                                          filename=...,
                                          save_to_file=...)
  
        data = datalogger.receive(lambda_gen=lambda x: "row_4" in x[0])

    """    
    def __init__(self, ip="192.168.66.213", filename="unnamed-experiment", save_to_file=True):
        self.context = zmq.Context()
        logger.info(f"Connecting to Publisher as Subscriber on {ip}:9872")
        self._setup_connection(ip)
        self.save_to_file = save_to_file
        self._file = None
        self._flush_time = time.perf_counter()

        if save_to_file:
            self.filename =  filename + datetime.datetime.now().strftime("-%Y-%m-%d_%H-%M-%S") + ".csv"
            path_file = pathlib.Path(self.filename)
            if not path_file.parent.exists():
                path_file.parent.mkdir(parents=True)
            self._setup_csv_file()

        self.header_written = False
        self.data = []

    def _setup_connection(self, ip):
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect(f"tcp://{ip}:9872")
        self.sub_socket.subscribe("")
        # self.req_socket.setsockopt(zmq.SUBSCRIBE)

    def _setup_csv_file(self, rotation=""):
        logger.info(f"Writing File: {self.filename+rotation}")
        if rotation != "":
            rotation = "." + rotation

        self._file =  open(self.filename+rotation, "w")
    
    def get_header(self, json_data):
        to_save = []
        flat_dict = utils.dict_flatten(json_data)

        for key, value in flat_dict.items():
            to_save.append(key)

        return to_save

    def get_data(self, dict_data):
        to_save = []
        to_data = []

        flat_dict = utils.dict_flatten(dict_data)

        for key, value in flat_dict.items():
            to_save.append(str(value))
            to_data.append(value)

        self.data = np.array(to_data[1:])

        return to_save

    def _write_header(self, dict_data):
        to_save = self.get_header(dict_data)
        self._write(",".join(to_save)+"\n")

    def receive(self, lambda_gen = None):
        raw_data = self.sub_socket.recv_json()

        deserialized_dict = utils.NpTypeDecoder(raw_data)
  
        data_arrays = {}

        if lambda_gen is not None:

            data_dict = { "timestamp": deserialized_dict["timestamp"],
                          **dict(filter(lambda_gen, deserialized_dict.items()))
                        }

        else:
            data_dict = deserialized_dict

        if not self.header_written and self.save_to_file:
            logger.info("Header written.")
            self._write_header(data_dict)
            self.header_written = True

        to_save = self.get_data(data_dict)

        if self.save_to_file:
            self._write(",".join(to_save)+"\n")
        
        return data_arrays

    def _write(self, data):
        self._file.write(data)

        current_time = time.perf_counter()

        if current_time - self._flush_time > 1:
            self._file.flush()
            self._flush_time = current_time
        
    def close(self):
        if self._file is not None:
            self._file.close()


class Publisher():
    def __init__(self, port=9872, pass_type=False):
        logger.debug("Init ZmqBasePublisher")
        self.pass_type = pass_type
        self.context = zmq.Context()
        self.skt = self.context.socket(zmq.PUB)
        self.skt.bind(f"tcp://*:{port}")

    def publish(self, data_dict):
        if self.pass_type:
            dtyped_values = utils.NpTypeEncoder(data)
            self.skt.send_json( json.dumps(dtyped_values, cls=utils.NpEncoder) )
        else:
            data = utils.dict_flatten(data_dict)
            self.skt.send_json( json.dumps(data, cls=utils.NpEncoder) )

