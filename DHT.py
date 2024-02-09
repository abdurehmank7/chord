import socket
import threading
from json import dumps, loads
import os
import time
import hashlib

'''
    WE NEED TO REPLICATE THE FILES AT OUR SUCCESSOR SINCE THE HASHING REMAINS CONSISTENT THAT WAY
    IF WE REPLICATE AT OUR PREDECESSOR, THE FILES WILL HAVE TO BE REHASHED, WHICH REQUIRES ADDITIONAL
    PROCESSING AND TIME
'''


FORMAT = "utf-8"

'''
FROM PA1
'''


def make_message(msg_type, msg_format, message=None):
    '''
    This function can be used to format your message according
    to any one of the formats described in the documentation.
    msg_type defines type like join, disconnect etc.
    msg_format is either 1,2,3 or 4
    msg is remaining.
    '''
    if msg_format == 2:
        return "%s" % (msg_type)
    if msg_format in [1, 3, 4]:
        return "%s %s" % (msg_type, message)
    return ""




class Node:
    def __init__(self, host, port):
        self.stop = False
        self.host = host
        self.port = port
        self.M = 16
        self.N = 2**self.M
        self.key = self.hasher(host + str(port))
        # You will need to kill this thread when leaving, to do so just set
        # self.stop = True
        threading.Thread(target=self.listener).start()
        self.files = []
        self.backUpFiles = []
        if not os.path.exists(host + "_" + str(port)):
            os.mkdir(host + "_" + str(port))
        '''
        ------------------------------------------------------------------------------------


        DO NOT EDIT ANYTHING ABOVE THIS LINE
        '''
        # Set value of the following variables appropriately to pass
        # Intialization test
        self.successor = (host, port)
        self.predecessor = (host, port)
        # additional state variables
        # this dict will have node key as val, and (host, port) as value
        self.connected_nodes = dict()
        self.watchDog = threading.Thread(target=self.ping).start()
        self.address_tuple = (self.host, self.port)


        self.next_hop = tuple()

    def hasher(self, key):
        '''
        DO NOT EDIT THIS FUNCTION.
        You can use this function as follow:
            For a node: self.hasher(node.host+str(node.port))
            For a file: self.hasher(file)
        '''
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % self.N

    def handleConnection(self, client, addr):
        '''
         Function to handle each inbound connection, called as a thread from the listener.
        '''
        # while not self.stop:
        # copy from PA1
        message_copy = client.recv(2048).decode(FORMAT)
        message = message_copy.split(" ", 1)

        if message[0] == "join":

            # print(message)
            node_info = loads(message[1])  # host, port
            # print(node_info)
            # print(type(node_info))

            node_info = (node_info[0], node_info[1])

            # send info to the node about the successor
            # print(node_info)
            successor = dumps(self.lookup(node_info))  # dumping a tuple
            # print("HandleConnection info returned")

            client.send(successor.encode(FORMAT))

        elif message[0] == "lookup":
            node_info = loads(message[1])
            node_info = (node_info[0], node_info[1])
            # print("Node Info", node_info)
            reply = dumps(self.lookup(node_info))  # dumping a tuple

            # reply = dumps(self.lookup(message[1])) #this will block as a
            # recursive search will take place
            client.send(reply.encode(FORMAT))

        elif message[0] == "send_predecessor":
            client.send(dumps(self.predecessor).encode(FORMAT))

        elif message[0] == "new_successor":
            node_info = loads(message[1])
            node_info = (node_info[0], node_info[1])
            self.successor = node_info
            # print("New Successor, Node:", self.address_tuple, "Predecessor:", self.predecessor, "Successor:", self.successor)

        elif message[0] == "new_predecessor":
            # print(message[1])
            # print(message[1])
            node_info = loads(message[1])

            node_info = (node_info[0], node_info[1])
            self.predecessor = node_info
            # print("New Predecessor, Node:", self.address_tuple, "Predecessor:", self.predecessor, "Successor:", self.successor)

        elif message[0] == "file_lookup":
            fileName = message[1]
            result = self.file_lookup(fileName)
            result = (result[0], result[1])
            client.send(dumps(result).encode(FORMAT))
            # print(self.address_tuple, result)

        elif message[0] == "recv_file":
            '''
                "localhost_20007/file.py"
                Arguments:	soc => a socket object
                    fileName => file's name including its path e.g. NetCen/PA3/file.py
            '''
            try:
                # print(message)
                fileName = message[1]
                store_path = self.host + "_" + str(self.port) + "/" + fileName
                # print(store_path)
                client.send("ok".encode(FORMAT))
                self.files.append(fileName)
                self.recieveFile(client, store_path)
            except BaseException:
                pass

            # file_hashes = []
            # for name in self.files:
            #     file_hashes.append(self.hasher(name))

            # print(file_hashes, self.key)

        elif message[0] == "get":

            content = message_copy.split(" ", 2)
            name = content[1]
            address = loads(content[2])
            # print(name, address)
            address = (address[0], int(address[1]))
            found = True if name in self.files else False

            # if not found: print("False")
            if found:
                for file in self.files:
                    if file == name:
                        client.send(name.encode(FORMAT))
                        try:
                            sock = socket.socket()
                            sock.connect(address)
                            message = make_message(
                                "recv_file", 1, name).encode(FORMAT)
                            sock.send(message)
                            blocker = client.recv(2048)
                            self.sendFile(sock, name)
                            sock.close()
                        except BaseException:
                            sock.close()
                        break

            else:
                client.send("n".encode(FORMAT))

        elif message[0] == "transfer_files":
            files_copy = self.files.copy()
            path = self.host + "_" + str(self.port) + "/"
            self.files = []

            for file in files_copy:
                store_path = path + file
                location = self.file_lookup(file)
                sock = socket.socket()
                sock.connect(location)
                message = make_message("recv_file", 1, file)
                sock.send(message.encode(FORMAT))
                blocker = sock.recv(1024)
                # try:
                self.sendFile(sock, store_path)
                # except:
                # pass
                sock.close()

        elif message[0] == "successor_leaving":
            new_successor = loads(message[1])
            self.successor = (new_successor[0], int(new_successor[1]))
            client.send("ok".encode(FORMAT))

        elif message[0] == "predecessor_leaving":
            new_predecessor = loads(message[1])
            self.predecessor = (new_predecessor[0], int(new_predecessor[1]))
            client.send("ok".encode(FORMAT))

        elif message[0] == "send_successor":
            client.send(dumps(self.successor).encode(FORMAT))

        elif message[0] == "backup":
            self.backUpFiles = message[1]
            client.send("ok".encode(FORMAT))

        elif message[0] == "restore":
            self.files = self.backUpFiles


            client.send("ok".encode(FORMAT))

        client.close()

    def listener(self):
        '''
        We have already created a listener for you, any connection made by  nodes will be accepted here.
        For every inbound connection we spin a new thread in the form of handleConnection function. You do not need
        to edit this function. If needed you can edit signature of handleConnection function, but nothing more.
        '''
        listener = socket.socket()
        listener.bind((self.host, self.port))
        listener.listen(10)
        while not self.stop:
            client, addr = listener.accept()
            threading.Thread(
                target=self.handleConnection, args=(
                    client, addr)).start()
        print("Shutting down node:", self.host, self.port)
        try:
            listener.shutdown(2)
            listener.close()
        except BaseException:


            listener.close()

    def join(self, joiningAddr):
        '''
        This function handles the logic of a node joining. This function should do a lot of things such as:
        Update successor, predecessor, getting files, back up files. SEE MANUAL FOR DETAILS.
        '''

        '''Joining addr contains host, port for a node already in the DHT'''
        if joiningAddr:
            # create socket to send :)
            sock = socket.socket()
            sock.connect(joiningAddr)
            message = "join " + dumps(self.address_tuple)
            # print(message)
            sock.send(message.encode(FORMAT))
            # print("Join request sent")
            successor = loads(sock.recv(2048).decode(FORMAT))
            # force convert list to tuple
            successor = (successor[0], successor[1])
            self.successor = successor

            # sock.send("send_predecessor".encode(FORMAT))
            # predecessor = loads(sock.recv(1024).decode(FORMAT))
            # self.predecessor = (predecessor[0], predecessor[1])
            sock.close()

        '''
        NOW WE HAVE TO ASK OUR SUCCESSOR THEIR PREDECESSOR. ONCE THE NODE INFORMS US, WE HAVE TO:
            1. CONTACT PREDECESSOR AND SEND AN "UPDATE SUCCESSOR MESSAGE"
            2. CONTACT SUCCESSOR AND SEND AN "UPDATE PREDECESSOR" MESSAGE

        ALTERNATIVELY, WE CAN WAIT FOR THE PINGING FUNCTION TO AUTOMATICALLY FIX THESE FOR US (WHICH IS WHAT I HAVE DONE)
        '''



    def put(self, fileName):
        '''
        This function should first find node responsible for the file given by fileName, then send the file over the socket to that node
        Responsible node should then replicate the file on appropriate node. SEE MANUAL FOR DETAILS. Responsible node should save the files
        in directory given by host_port e.g. "localhost_20007/file.py".
        '''
        sock = socket.socket()
        try:
            place = self.file_lookup(fileName)
            sock.connect(place)
            message = make_message("recv_file", 1, fileName)
            sock.send(message.encode(FORMAT))
            # to prevent sending file before  node is ready for it
            blocker = sock.recv(4096)
            self.sendFile(sock, fileName)
        except BaseException:
            pass



        sock.close()

    def get(self, fileName):
        '''
        This function finds node responsible for file given by fileName, gets the file from responsible node, saves it in current directory
        i.e. "./file.py" and returns the name of file. If the file is not present on the network, return None.
        '''

        # why are there so many noneType errors
        if fileName:

            loc = self.file_lookup(fileName)
            # print(loc)
            sock = socket.socket()
            sock.connect(loc)

            s = fileName + " " + dumps(self.address_tuple)

            message = make_message("get", 1, s).encode(FORMAT)
            sock.send(message)

            name = sock.recv(2048).decode(FORMAT)  # blocking
            sock.close()

            if fileName == name:
                return fileName



            return None

    def leave(self):
        '''
        When called leave, a node should gracefully leave the network i.e. it should update its predecessor that it is leaving
        it should send its share of file to the new responsible node, close all the threads and leave. You can close listener thread
        by setting self.stop flag to True
        '''

        '''
            PREDECESSOR -> NODE -> SUCCESSOR
        '''

        # print(self.predecessor, self.address_tuple, self.successor)

        # update predecessor and successor values first to prevent ping
        # function from breaking the ring
        successor = self.successor
        predecessor = self.predecessor
        self.predecessor = (self.host, self.port)
        self.successor = (self.host, self.port)

        # create sockets for both predecessor and successor
        predecessor_socket = socket.socket()
        predecessor_socket.connect(predecessor)
        successor_socket = socket.socket()
        successor_socket.connect(successor)

        # inform predecessor we are leaving and send them our successor
        message = make_message(
            "successor_leaving",
            1,
            dumps(successor)).encode(FORMAT)
        predecessor_socket.send(message)
        blocker = predecessor_socket.recv(2048)
        # print("successor leaving callef from leave")
        predecessor_socket.close()

        # inform successor we are leaving and send them our predecessor and all
        # files
        message2 = make_message(
            "predecessor_leaving",
            1,
            dumps(predecessor)).encode(FORMAT)
        successor_socket.send(message2)
        blocker2 = successor_socket.recv(2048)
        # print("predecessor leaving callef from leave")
        successor_socket.close()

        path = self.host + "_" + str(self.port) + "/"

        for file in self.files:
            # print(file)
            sock = socket.socket()
            sock.connect(successor)
            send_path = path + file
            message = make_message("recv_file", 1, file).encode(FORMAT)
            sock.send(message)
            # print(message)
            blocker = sock.recv(512)
            # print("here")
            self.sendFile(sock, send_path)
            sock.close()

        self.stop = True



    def sendFile(self, soc, fileName):
        '''
        Utility function to send a file over a socket
            Arguments:	soc => a socket object
                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = os.path.getsize(fileName)
        soc.send(str(fileSize).encode('utf-8'))
        soc.recv(1024).decode('utf-8')
        with open(fileName, "rb") as file:
            contentChunk = file.read(1024)
            while contentChunk != "".encode('utf-8'):
                soc.send(contentChunk)
                contentChunk = file.read(1024)

    def recieveFile(self, soc, fileName):
        '''
        Utility function to recieve a file over a socket
            Arguments:	soc => a socket object
                        fileName => file's name including its path e.g. NetCen/PA3/file.py
        '''
        fileSize = int(soc.recv(1024).decode('utf-8'))
        soc.send("ok".encode('utf-8'))
        contentRecieved = 0
        file = open(fileName, "wb")
        while contentRecieved < fileSize:
            contentChunk = soc.recv(1024)
            contentRecieved += len(contentChunk)
            file.write(contentChunk)
        file.close()

    def kill(self):
        # DO NOT EDIT THIS, used for code testing
        self.stop = True

    def lookup(self, addr):
        '''
            THIS FUNCTION RETURNS THE NODE RESPONSIBLE FOR THE KEY
        '''
        key = self.key
        node_key = self.hasher(addr[0] + str(addr[1]))
        successor_key = self.hasher(self.successor[0] + str(self.successor[1]))
        keys = [key, node_key, successor_key]
        # print(keys)
        # return self.successor

        if (node_key > key and node_key < successor_key) or (
                (key > successor_key) and node_key == min(keys)):
            # if key < successor_key and successor_key < node_key:
            # print("1st if")
            return self.successor

        elif ((key > successor_key) and node_key == max(keys)) or key == successor_key:
            # print("2nd if")
            return self.successor

        else:
            '''
            CALL SUCCESSORS OF SUCCESOR RECURSIVELY TO SEE BROADCAST LOCATION TO NEW NODE
            REFER TO SLIDES
            '''
            # print("3rd if")
            successor_socket = socket.socket()
            successor_socket.connect(self.successor)

            message = make_message("lookup", 1, dumps(addr)).encode(FORMAT)
            successor_socket.send(message)

            successor = successor_socket.recv(2048).decode(FORMAT)
            successor = loads(successor)
            successor_tuple = (successor[0], successor[1])

            successor_socket.close()

            return successor_tuple

    def ping(self):



        while not self.stop:
            # print("Here")
            time.sleep(0.5)
            try:
                # connect to successor to get predecessor
                sock = socket.socket()
                sock.connect(self.successor)
                sock.send("send_predecessor".encode(FORMAT))
                predecessor = loads(sock.recv(2048).decode(FORMAT))
                predecessor = (predecessor[0], predecessor[1])
                sock.close()

                if not self.address_tuple == predecessor:
                    # inform predecessor that we are their successor by sending
                    # a successor message
                    self.predecessor = predecessor
                    pred = socket.socket()
                    pred.connect(self.predecessor)
                    message = make_message(
                        "new_successor", 1, dumps(
                            self.address_tuple)).encode(FORMAT)
                    pred.send(message)
                    pred.close()

                    # inform our successor we are their new predecessor
                    sock = socket.socket()
                    sock.connect(self.successor)
                    message = make_message(
                        "new_predecessor", 1, dumps(
                            self.address_tuple)).encode(FORMAT)
                    sock.send(message)
                    sock.close()  # to prevent errors as new predecessor and transfer files was being sent together

                    sock = socket.socket()
                    sock.connect(self.successor)
                    sock.send("transfer_files".encode(FORMAT))
                    sock.close()

                sock2 = socket.socket()
                try:
                    sock2.connect(self.successor)
                    message = make_message(
                        "backup", 1, dumps(
                            self.files)).encode(FORMAT)
                    sock2.send(message)
                    blocker = sock2.recv(2048)
                    sock2.close()
                except BaseException:
                    sock2.close()
                    pass

                # KEEP TRACK OF THE NEXT HOP IN CASE OF NODE FAILURE
                sock2 = socket.socket()
                try:
                    sock2.connect(self.successor)
                    sock2.send("send_successor".encode(FORMAT))
                    next_hop = loads(sock2.recv(2048).decode(FORMAT))
                    self.next_hop = (next_hop[0], int(next_hop[1]))
                    sock2.close()
                except BaseException:
                    pass

                sock.close()
                sock2.close()

            except BaseException:
                # print("Here")
                sock.close()
                self.successor = self.next_hop

                # inform next hop we are their predecessor
                s = socket.socket()
                s.connect(self.next_hop)
                message = make_message(
                    "new_predecessor", 1, dumps(
                        self.address_tuple)).encode(FORMAT)
                s.send(message)
                s.close()

                s = socket.socket()
                s.connect(self.next_hop)
                s.send("restore".encode(FORMAT))
                blocker = s.recv(2048)
                s.close()

                # sock.close()

            # print("================================")
            # print("Node:", self.address_tuple, "Predecessor:", self.predecessor, "Successor:", self.successor)
            # print("================================")

    def file_lookup(self, fileName):
        '''
            THIS FUNCTION IS RESPONSIBLE FOR RETURNING THE NODE WHERE A FILE WITH NAME fileName HAS TO BE STORED
        '''

        key = self.key


        file_key = self.hasher(fileName)
        successor_key = self.hasher(self.successor[0] + str(self.successor[1]))
        keys = [key, file_key, successor_key]

        if (file_key > key and file_key < successor_key) or (
                (key > successor_key) and file_key == min(keys)):
            # print("1st if")
            return self.successor

        elif ((key > successor_key) and file_key == max(keys)) or key == successor_key:
            # print("2nd if")
            return self.successor

        else:
            '''
            CALL SUCCESSORS OF SUCCESOR RECURSIVELY TO FIND NODE WHICH HAS TO STORE FILE
            '''
            # print("3rd if")
            # print(fileName)
            successor_socket = socket.socket()
            successor_socket.connect(self.successor)

            message = make_message("file_lookup", 1, fileName).encode(FORMAT)
            successor_socket.send(message)

            store = loads(successor_socket.recv(2048).decode(FORMAT))

            node_tuple = (store[0], store[1])

            successor_socket.close()

            return node_tuple