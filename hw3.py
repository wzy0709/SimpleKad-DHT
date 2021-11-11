#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import grpc
from six import b

import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc

from collections import OrderedDict
import math
buckets = [[] for i in range(4)]
data = {}
# print the buckets
def output_bucket():
	for i in range(4):
		print("%i:"% i, end='')
		for node in buckets[i]:
			print(" %i:%i"% (node.id, node.port), end='')
		print("")
# update node 
def update_node(remote_node):
	distance = my_node.id ^ remote_node.id
	i = int(math.log2(distance))
	bucket = buckets[i]
	# delete repeat
	for i in range(len(bucket)):
		if bucket[i].id == remote_node.id:
			bucket.pop(i)
			break
	# full
	if (len(bucket)>=k):
		bucket.pop(0)
	bucket.append(remote_node)
# get distance dictionary
def get_distance_dictionary(number):
	distance_dict = {}
	# sort distance
	for i in range(4):
		for j in range(len(buckets[i])):
			distance = buckets[i][j].id ^ number
			distance_dict[distance] = buckets[i][j]
	return OrderedDict(sorted(distance_dict.items()))
# get nodes list
def get_k_closest(distance_dict):
	l = []
	items = list(distance_dict.items())
	for i in range(min(k,len(distance_dict))):
		l.append(items[i][1])
	return l

def bootstrap(remote_addr_name, remote_port):
	# get remote info
	remote_addr = socket.gethostbyname(remote_addr_name)
	remote_port = int(remote_port)
	# connect
	channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))
	stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
	# find self
	nodelist = stub.FindNode(csci4220_hw3_pb2.IDKey(node = my_node, idkey = my_node.id))
	# save remote to buckets
	update_node(nodelist.responding_node)
	for node in nodelist.nodes:
		if get_node_by_id(node.id)==None:
			update_node(node)
	channel.close()
	# print buckets
	print("After BOOTSTRAP(%i), k-buckets are:"% (nodelist.responding_node.id))
	output_bucket()

# connect to node
def connect_node(node):
	channel = grpc.insecure_channel(node.address + ':' + str(node.port))
	stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
	return [channel, stub]

# find node by in
def get_node_by_id(id):
	distance = my_node.id ^ id
	# self
	if distance == 0:
		return my_node
	i = int(math.log2(distance))
	for node in buckets[i]:
		# node found
		if id == node.id:
			return node
	return None
# find helper
def find_helper(number, type):
	asked = set()
	asked.add(my_node.id)
	distance_dict = get_distance_dictionary(number)
	# while not asked k
	while(1):
		nodes = list(distance_dict.values())
		ask_count = 0
		for node in nodes:
			if node.id not in asked:
				asked.add(node.id)
				update_node(node)
				# connect to node
				[channel, stub] = connect_node(node)
				# find node
				if type == "node":
					# request k node
					R = stub.FindNode(csci4220_hw3_pb2.IDKey(node = my_node, idkey = number))
				# find value
				else:
					R = stub.FindValue(csci4220_hw3_pb2.IDKey(node = my_node, idkey = number))
					# value found
					if R.mode_kv==1:
						return R.kv.value
				for n in R.nodes:
					if n.id not in asked:
						distance = n.id ^ number
						distance_dict[distance] = n
						OrderedDict(sorted(distance_dict.items()))
						if get_node_by_id(n.id)==None:
							update_node(n)
				channel.close()
				# node Found
				if type == "node" and get_node_by_id(number)!=None:
					return True
			# asked
			else:
				ask_count = ask_count + 1
		# could not found
		if ask_count >= min(k,len(nodes)):
			return False
# find node
def find_node(target_node_id):
	print("Before FIND_NODE command, k-buckets are:")
	output_bucket()
	found = find_helper(target_node_id, "node")
	if found:
		print("Found destination id %i"% target_node_id)
	else:
		print("Could not find destination id %i"%target_node_id)
	print("After FIND_NODE command, k-buckets are:")
	output_bucket()
# find value
def find_value(key):
	print("Before FIND_VALUE command, k-buckets are:")
	output_bucket()
	# already stored
	if key in data:
		print("Found data \"%s\" for key %i"%(data[key].value, key))
	else:
		result = find_helper(key, "value")
		# found
		if result!=False:
			print("Found value \"%s\" for key %i"% (result, key))
		# could not found
		else:
			print("Could not find key %i"%key)
	print("After FIND_VALUE command, k-buckets are:")
	output_bucket()
# store
def store(key, value):
	request = csci4220_hw3_pb2.KeyValue(node = my_node, key = key, value = value)
	# get closest node
	distance_dict = get_distance_dictionary(key)
	list = get_k_closest(distance_dict)
	if len(list)==0:
		remote_node = None
	else:
		remote_node = list[0]
	# save locally
	if (not remote_node) or (remote_node.id ^ key > my_node.id ^ key):
		print("Storing key %i at node %i"% (key, my_node.id))
		data[key] = request
		return
	# store remotely
	print("Storing key %i at node %i"% (key, remote_node.id))
	# store
	[channel, stub] = connect_node(remote_node)
	stub.Store(request)
	channel.close()
# quit
def quit():
	for bucket in buckets:
		for node in bucket:
			print("Letting %i know I'm quitting."%node.id)
			[channel, stub] = connect_node(node)
			try:
				stub.Quit(csci4220_hw3_pb2.IDKey(node = my_node, idkey = my_node.id))
			except:
				pass
			channel.close()
	print("Shut down node %i"% my_node.id)
class KadImplServicer(csci4220_hw3_pb2_grpc.KadImplServicer):
	# """Kadmelia implementation for CSCI-4220
	# """
	def __init__(self):
		pass
	def FindNode(self, request, context):
		requesting_node = request.node
		# print ack
		print("Serving FindNode(%i) request for %i"%(request.idkey,requesting_node.id))
		dict = get_distance_dictionary(request.idkey)
		list = get_k_closest(dict)
		update_node(requesting_node)
		return csci4220_hw3_pb2.NodeList(responding_node = my_node, nodes = list)
	def FindValue(self, request, context):
		print("Serving FindKey(%i) request for %i"%(request.idkey, request.node.id))
		nodes = []
		kv = csci4220_hw3_pb2.KeyValue()
		# return value
		if request.idkey in data:
			mode_kv = 1
			kv = data[request.idkey]
		# return closest
		else:
			mode_kv = 0
			dict = get_distance_dictionary(request.idkey)
			nodes = get_k_closest(dict)
		update_node(request.node)
		return csci4220_hw3_pb2.KV_Node_Wrapper(responding_node = my_node, mode_kv = mode_kv,
												kv = kv, nodes = nodes)

	def Store(self, request, context):
		print("Storing key %i value \"%s\""%(request.key, request.value))
		data[request.key] = request
		update_node(request.node)
		return csci4220_hw3_pb2.IDKey()

	def Quit(self, request, context):
		request_id = request.idkey
		distance = request_id ^ my_node.id
		i = int(math.log2(distance))
		for j in range(len(buckets[i])):
			# node found
			if request_id == buckets[i][j].id:
				print("Evicting quitting node %i from bucket %i"%(request_id, i))
				buckets[i].pop(j)
				return request
		print("No record of quitting node %i in k-buckets."%request_id)
		return request
def start():
	while(1):
		i = input()
		args = i.split()
		command = args[0]
		if command == 'BOOTSTRAP':
			remote_addr_name = args[1]
			remote_port = int(args[2])
			bootstrap(remote_addr_name, remote_port)
		elif command == 'FIND_NODE':
			target_node_id = int(args[1])
			find_node(target_node_id)
		elif command == 'FIND_VALUE':
			key = int(args[1])
			find_value(key)
		elif command == 'STORE':
			key = int(args[1])
			value = args[2]
			store(key, value)
		elif command == 'QUIT':
			quit()
			break
		# not defined
		else:
			pass
			

def run():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	local_id = int(sys.argv[1])
	my_port = int(sys.argv[2]) # add_insecure_port() will want a string
	global k
	k = int(sys.argv[3])
	my_hostname = socket.gethostname() # Gets my host name
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname
	# start the server
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=15))
	global my_node
	my_node = csci4220_hw3_pb2.Node(id = local_id, port = my_port, address = my_address)
	csci4220_hw3_pb2_grpc.add_KadImplServicer_to_server(KadImplServicer(), server)
	server.add_insecure_port(my_address + ':' + str(my_port))
	server.start()
	start()

if __name__ == '__main__':
	run()
