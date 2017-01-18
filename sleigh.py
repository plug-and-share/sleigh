'''
Copyright (c) 2016-2017 Plug-and-share
All rights reserved.
The license below extends only to copyright in the software and shall
not be construed as granting a license to any other intellectual
property including but not limited to intellectual property relating
to a hardware implementation of the functionality of the software
licensed hereunder.  You may use the software subject to the license
terms below provided that you ensure that this notice is replicated
unmodified and in its entirety in all distributions of the software,
modified or unmodified, in source code or in binary form.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met: redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer;
redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution;
neither the name of the copyright holders nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

VERSION        v0.0.0
DATE           18/01/2017
AUTHORS        TASHIRO
PYTHON_VERSION v3
'''
import pickle
import select
import socket
import time

import schedule

class Sleigh:

	EOF = b'\n\r\t'

	def __init__(self, param, method_name, vm_img, port, processing_time_limit=0, max_wait_time=0, check_cycle=60):
		print('--- Sleigh.__init__')
		self.param = param 
		self.vm_img = vm_img
		self.method_name = method_name
		self.processing_time_limit = processing_time_limit
		self.max_wait_time = max_wait_time
		self.check_cycle = check_cycle
		self.last_time_checked = time.time()
		self.active_collaborators = {}
		self.deactivated_collaborators = {}
		self.schedule = getattr(schedule.Schedule(param), method_name)()
		self.still_have_instuctions = True
		self.sock = socket.socket() 
		self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.sock.bind(('localhost', port))
		self.sock.listen(5)
		self.epoll = select.epoll()
		self.epoll.register(self.sock.fileno(), select.EPOLLIN)
		self.conns = {}
		self.req = {}
		self.resp = {}

	def run(self):
		print('--- Sleigh.run')
		try:
			while 1:
				events = self.epoll.poll(1)
				for fileno, event in events:
					if fileno == self.sock.fileno():
						conn, addr = self.sock.accept()
						conn.setblocking(0)
						self.epoll.register(conn.fileno(), select.EPOLLIN)
						self.conns[conn.fileno()] = conn
						self.req[conn.fileno()] = b''
					elif event & select.EPOLLIN:
						self.req[fileno] += self.conns[fileno].recv(1024)
						if Sleigh.EOF in self.req[fileno]:						
							self.resp[fileno] = self.action(self.req[fileno][:-3], self.conns[fileno])
							if self.resp[fileno] != None:
								self.epoll.modify(fileno, select.EPOLLOUT)
							else:
								self.epoll.unregister(fileno)
								self.conns[fileno].close()
								del self.conns[fileno]
					elif event & select.EPOLLOUT:
						bw = self.conns[fileno].send(self.resp[fileno])
						self.resp[fileno] = self.resp[fileno][bw:]
						if len(self.resp[fileno]) == 0:
							self.epoll.modify(fileno, 0)
							self.conns[fileno].shutdown(socket.SHUT_RDWR)
					elif event & select.EPOLLHUP:
						self.epoll.unregister(fileno)
						self.conns[fileno].close()
						del self.conns[fileno]
				for collaborator in active_collaborators:
					pass
				for collaborator in deactivated_collaborators:
					pass
		finally:
			self.epoll.unregister(self.sock.fileno())
			self.epoll.close()
			self.sock.close()

	def new_collaborator(self, conn):
		print('--- Sleigh.new_collaborator')
		if self.processing_time_limit:
			self.active_collaborators[conn.getpeername()] = time.time()
		else:
			self.active_collaborators[conn.getpeername()] = 0
		return '\x42' + self.method_name.encode() + b' ' +  self.vm_img.encode() + Sleigh.EOF

	def ask_to_descollaboration(self, conn):
		if conn.getpeername() in self.active_collaborators:
			del self.active_collaborators[conn.getpeername()]
		elif conn.getpeername() in self.deactivated_collaborators:
			del self.deactivated_collaborators[conn.getpeername()]

	def instruction_request(self, conn):
		print('--- Sleigh.instruction_request')
		try:
			return pickle.dumps(next(self.schedule))
		except StopIteration:
			return '\x43' + Sleigh.EOF

	def results(self, payload):
		print('--- Sleigh.results')
		print(payload)
		if not self.still_have_instuctions:
			 input('DEBUG: Todas as instrucoes foram processadas')

	def action(self, msg, conn):
		print('Sleigh.action')
		code, payload = msg[:1], msg[1:]
		if code == b'\x03':
			return self.new_collaborator(conn)
		elif code == b'\x04':
			return self.ask_to_descollaboration(conn)
		elif code == b'\x05':
			return self.instruction_request(conn)
		elif code == b'\x06':
			return self.results(payload)

if __name__ == '__main__':
	param = {'param_one': (1, 2, 3), 'param_two': (4, 5, 6), 'param_four': (7, 8, 9)}
	method_name = 'schedule'
	vm_img = '655.626.652'
	Sleigh = Sleigh(param, method_name, vm_img, 50000)
	Sleigh.run()
