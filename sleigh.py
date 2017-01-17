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
DATE           17/01/2017
AUTHORS        TASHIRO
PYTHON_VERSION v3
'''
import select
import socket

class Sleigh:

	EOF = b'\n\r\t'

	def __init__(self, port):
		print('Sleigh.__init__')
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
		print('Sleigh.run')
		try:
			while 1:
				events = self.epoll.poll(1)
				for fileno, event in events:
					if fileno == self.sock.fileno():
						conn, addr = self.sock.accept()
						print('DEBUG: Sleigh.run', addr)
						conn.setblocking(0)
						self.epoll.register(conn.fileno(), select.EPOLLIN)
						self.conns[conn.fileno()] = conn
						self.req[conn.fileno()] = b''
					elif event & select.EPOLLIN:
						self.req[fileno] += self.conns[fileno].recv(1024)
						print('DEBUG: Sleigh.run.EPOLLIN', self.req[fileno])
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
		finally:
			self.epoll.unregister(self.sock.fileno())
			self.epoll.close()
			self.sock.close()

	def collaboration_request(self, conn):
		print('Sleigh.colloboration_request')

	def descollaboration_request(self, conn):
		print('Sleigh.descollaboration_request')

	def instruction_request(self, conn):
		print('Sleigh.instruction_request')

	def results(self, conn):
		# ...
		#if self.finish:
			# encerrar o processo do broker

	def action(self, msg, conn):
		print('Sleigh.action')
		code, payload = msg[:1], msg[1:]
		if code == b'\x03':
			return self.collaboration_request(conn)
		elif code == b'\x04':
			return self.descollaboration_request(conn)
		elif code == b'\x05':
			return self.instruction_request(conn)

if __name__ == '__main__':
	sleigh = Sleigh(50000)
	sleigh.run()
