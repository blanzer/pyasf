import zmq
import json
from loguru import logger


class _NpEncoder(json.JSONEncoder):
    """Object to serialize our dicts into json file"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.dtype):
            return str(obj)  
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)   


class ControllerLink():
    """Handles the communication with the asf controller.
    
    Every message send in dict gets acknoledged by the controller."""

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
        self.req_socket.send_json(json.dumps(data, cls=_NpEncoder))
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
