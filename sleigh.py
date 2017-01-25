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

import collaborator
import schedule

class Sleigh:
	'''
	TODO: 
	-Possiveis novas ações:
		.list_collaborators() - Envia para o pine a lista de colaboradores, associando 
		 para cada um se esta ativo ou desativo, além do tempo que ficou colaborando.		 
		.progress() - Retorna o quanto ja foi processado
	.Possiveis novos metodos:	
		.setup() - Verifica se todos os parametros passados são validos.
		.generate_report() - gera as estatisticas pertinentes durante o pŕocessamento 
		 da aplicação
	.Avisar o Sleigh caso o computador seja desligado
	'''	
	EOF = b'\n\r\t'

	def __init__(self, param, method_name, vm_img, port, processing_time_limit=0, max_wait_time=0, check_cycle=60, db_address=None):
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
		self.db_address = db_address

	def run(self):
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
				actual_time = time.time()
				if actual_time - self.last_time_checked > self.check_cycle:
					if self.processing_time_limit != 0:
						for collaborator in self.active_collaborators.items():
							if actual_time - collaborator[1].actual_time > self.processing_time_limit:
								# TODO: Resgatar a instrução e colocar de volta no schedule				
								self.deactivated_collaborators[collaborator[0]] = collaborator
								self.deactivated_collaborators[collaborator[0]].actual_time = actual_time
								del self.active_collaborators[collaborator[0]]
								self.still_have_instuctions = True
					if self.max_wait_time != 0:
						for collaborator in self.deactivated_collaborators.items():
							if actual_time - collaborator[1].actual_time > self.max_wait_time:
								del self.deactivated_collaborators[collaborator[0]]
					self.last_time_checked = actual_time
		finally:
			self.epoll.unregister(self.sock.fileno())
			self.epoll.close()
			self.sock.close()

	def new_collaborator(self, msg, conn):
		'''
		Um colaborador é uma máquina que vai ajudar no processamento de alguma
		aplicação.

		1° passo: O pine envia uma requisição para colaborar com essa aplicação.

		2° passo: O sleigh adiciona o endereço do pine na lista de colaboradores
				  inativos. Caso o sleigh tenha configurado um tempo limite de 
				  inatividade é associado ao pine o tempo atual, para depois ver 
				  se esse tempo excedeu o permitido. Se isso acontecer ele é re-
				  tirado da lista de colaboradores. Senão o valor zero é associ-
				  ado indicando que não tem um tempo limite.

		3° passo: O sleigh manda uma mensagem para o pine confirmando a requisi-
				  ção e enviando qual método a aplicação está usando além de qual 
				  o identificador da imagem da máquina virtual. 
		'''
		if self.max_wait_time != 0:
			self.deactivated_collaborators[conn.getpeername()] = collaborator.Collaborator(time.time())
		else:
			self.deactivated_collaborators[conn.getpeername()] = collaborator.Collaborator(0)
 		return self.method_name.encode() + b' ' +  self.vm_img.encode() + Sleigh.EOF

	def ask_to_descollaboration(self, conn):
		'''
		1° passo: O pine envia uma requisição para descolaborar com essa aplicação.

		2° passo: O sleigh retira da lista de colaboradores. Caso ele esteja na lis-
				  ta de colaboradores ativos, sua instrução atual é resgatada para 
				  posteriormente ser enviada para outro colaborador, então ele é 
				  retirado da lista de colaboradores. Caso esteja na lista de cola=
				  boradores desativos, ele é simplismente tirado da lista.

		3° passo: O sleigh manda um feedback confirmando que a ação foi realizada e
				  ele não mais faz parte da lista de colaboradores.
		'''
		if conn.getpeername() in self.active_collaborators:
			del self.active_collaborators[conn.getpeername()]
		elif conn.getpeername() in self.deactivated_collaborators:
			del self.deactivated_collaborators[conn.getpeername()]
		return b'\x43' + Sleigh.EOF

	def send_gift(self, conn):
		'''
		1° passso: Gerar uma nova instrução e serializar ela, permitindo o envio
				   pela rede. 

		2° passo: Associar essa instrução com o colaborador que solicitou, permi-
				  tindo resgata-la caso ele não termine de processa-la. Também o
				  é atualizado o tempo do colaborador.

		3° passo: A instrução é enviada para o colaborador. Caso todas as instu-
				  ções já foram processadas é enviado uma mensagem avisando este
				  fato.
		'''
		try:
			gift = pickle.dumps(next(self.schedule))
			if conn.getpeername() in self.deactivated_collaborators:
				self.active_collaborators[conn.getpeername()] = self.deactivated_collaborators[conn.getpeername()]			
				self.active_collaborators[conn.getpeername()].actual_time = time.time()
				self.active_collaborators[conn.getpeername()].gift = gift
				del self.deactivated_collaborators[conn.getpeername()]
			return gift			
		except StopIteration:
			if conn.getpeername() in self.active_collaborators:
				del self.active_collaborators[conn.getpeername()]
				# jogar par lista de desativo
			self.still_have_instuctions = False
			return '\x43' + Sleigh.EOF

	def results(self, payload, conn):
		'''
		1° passo: Adicionar uma nova conexão com o banco de dados e registra na 
				  poll das conexões para o resultado ser enviado posteriormente.

		*2° passo: Caso as instruções tenham acabado é avisado para cada colabora-
                   que as instruções acabaram. Então o sleigh é encerrado.
		'''
		self.active_collaborators[conn.getpeername()].total_collaboration_time += time.time() - self.active_collaborators[conn.getpeername()].actual_time
		# jogar para lista dos desativos
		conn = socket.socket()
		conn.connect(self.db_address)
		conn.setblocking(0)
		self.epoll.register(conn.fileno(), select.EPOLLOUT)
		self.conns[conn.fileno()] = conn
		self.resp[conn.fileno()] = payload
		if not self.still_have_instuctions and len(self.active_collaborators) == 0:
			for collaborator in self.deactivated_collaborators.items():
			 	self.conn = socket.socket()
			 	self.conn.connect(collaborator[0])
			 	self.conn.setblocking(0)
				self.epoll.register(conn.fileno(), select.EPOLLOUT)
				self.conns[conn.fileno()] = conn
				self.resp[conn.fileno()] = b'\x55' + Sleigh.EOF  # checar melhor esse código
			 print('Sleigh accomplish its fate. Now it are not longer here.')

	def action(self, msg, conn):
		code, payload = msg[:1], msg[1:]
		if code == b'\x03':
			return self.new_collaborator(msg, conn)
		elif code == b'\x04':
			return self.ask_to_descollaboration(conn)
		elif code == b'\x05':
			return self.send_gift(conn)
		elif code == b'\x06':
			return self.results(payload)

if __name__ == '__main__':
	param = {'param_one': (1, 2, 3), 'param_two': (4, 5, 6), 'param_four': (7, 8, 9)}
	method_name = 'schedule'
	vm_img = '655.626.652'
	Sleigh = Sleigh(param, method_name, vm_img, 50000)
	Sleigh.run()
