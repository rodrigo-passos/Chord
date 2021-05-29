from hashlib import sha1
import threading
import socket
import select


class No_chord:
    # Configurações do nó chord
    HOST = ''
    encoding = "UTF-8"


    def __init__(self, porta_no, id_no, n_nos_entrada, prox_no):
        """ Incio da classe dos Nós chords
        """
        self.porta_do_no = porta_no # porta do nó 
        self.id_do_no = id_no # id do nó 
        self.n_nos_entrada = n_nos_entrada # numero de nos de entrada
        self.lista_select = [] # lista de select
        self.tabela_hash = {}  # armazena os pares chave/valor do nó
        self.prox_no_da_tabela = prox_no  # armazena o nó mais próximo a determinada distância
        self.__No_Inicio()


    def Inicia_Socket(self):
        """ Inicia o socket para receber novas conexões de nós chords
        """
        sock_final = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_final.bind((self.HOST, self.porta_do_no))
        sock_final.listen(self.n_nos_entrada + 1)  # espera por até n_nos_entrada + 1 conexões
        sock_final.setblocking(False)  # Desbloqueia o socket na espera por conexões
        print('[NÓ ' +str(self.id_do_no) +'] - Inicio do Socket ' + str([self.HOST, self.porta_do_no, self.n_nos_entrada]))
        
        return sock_final

  
    def Inicia_Conexao(self, porta):
        """ Faz a conexão com um outro nó a partir de outro do anel
        """
        print('[NÓ %s] - Inicio da conexão da porta %s' % (self.id_do_no, porta))
        novo_socket = socket.socket()
        novo_socket.connect((self.HOST, porta))
        print('[NÓ %s] - Conectado com exito.' % (self.id_do_no))
    
        return novo_socket


    def verifica_Hash(self, chave):
        """ Verifica se a hash pertence ao nó em questão e retorna a hash ou o nó correto
        """
        palavra_hash = sha1(str.encode(chave)).hexdigest()
        no_geral = 16
        no_obj = int(palavra_hash, 16) % no_geral
        if no_obj == self.id_do_no:
            return True, palavra_hash
        else:
            return False, no_obj


    def encontra_distancia_do_no(self, no_obj, valor):
        """ Verifica a distância entre dois nós especificados no anel
        """
        no_Aux = self.id_do_no + (2 ** valor) % (16)
        distancia_obj = (no_obj - self.id_do_no) if (no_obj > self.id_do_no) else (no_obj - self.id_do_no + 16)
        distancia_do_no = (no_Aux - self.id_do_no) if (no_Aux > self.id_do_no) else (no_Aux - self.id_do_no + 16)
        
        return distancia_obj - distancia_do_no


    def Move_para_no_correto(self, no_obj, mensagem):
        """ Função de redirecionamento da requisição, caso não seja o nó correto
        """
        no_conectado = -1
        porta_no = -1
        for i in range(len(self.prox_no_da_tabela)):
            if self.encontra_distancia_do_no(no_obj, i) < 0:
                no_conectado = i - 1
                porta_no = self.prox_no_da_tabela[no_conectado]
                break
            elif i == len(self.prox_no_da_tabela) - 1:
                no_conectado = i
                porta_no = self.prox_no_da_tabela[no_conectado]

        print('[NÓ %s] - Movendo para o nó %s.' % (self.id_do_no, no_conectado))   
        no_socket = self.Inicia_Conexao(porta_no)
        no_socket.send(mensagem.encode(self.encoding))
        print('[NÓ %s] - Encerrando conexão.' % (self.id_do_no))
        no_socket.close()
        

    def busca(self, porta_cliente, chave, mensagem):
        """ Busca o valor associado a uma chave na tabela
        """
        print('[NÓ %s] - Averiguando se a chave %s pertence ao nó.' % (self.id_do_no, chave))
        esta_correto, data = self.verifica_Hash(chave)

        if esta_correto:
            try:
                print('[NÓ %s] - Buscando chave inserida' % (self.id_do_no))
                valor = self.tabela_hash[data]
                anexo = "((FEITO))"
                mensagem_enviada = anexo + "%s - %s" % (self.id_do_no, valor)
            except:
                print('[NÓ %s] - Chave %s não existe.' % (self.id_do_no, chave))
                anexo = "((falha))"
                mensagem_enviada = anexo
            finally:
                cliente_socket = self.Inicia_Conexao(int(porta_cliente))
                cliente_socket.send(mensagem_enviada.encode(self.encoding))
                print('[NÓ %s] - Encerrando conexão.' % (self.id_do_no))
                cliente_socket.close()

        else:
            self.Move_para_no_correto(data, mensagem)


    def insere(self, chave, valor, mensagem):
        """ Insere na tabela hash a chave e valor
        """
        print('[NÓ %s] - Averigua se a chave %s esta no no' % (self.id_do_no, chave))
        esta_certo, data = self.verifica_Hash(chave)
        if esta_certo:
            print('[NÓ %s] - Chave %s salva.' % (self.id_do_no, chave))
            self.tabela_hash[data] = valor
        else:
            self.Move_para_no_correto(data, mensagem)


    def Verifica_Comando(self, mensagem):
        """ Verifica qual o comando a ser executado na proxima etapa
        """
        # Filtra a Mensagem original
        comeco_anexo = mensagem.index('((')
        fim_anexo = mensagem.index('))')
        identifica_comando = mensagem[comeco_anexo + 2:fim_anexo]
        msg_filtrada = mensagem[fim_anexo + 2:]

        if identifica_comando == 'busca':
            print('[NÓ %s] - Buscando' % (self.id_do_no))
            index_msg = msg_filtrada.index(' - ')
            self.busca(msg_filtrada[:index_msg], msg_filtrada[index_msg + 3:], mensagem)
        elif identifica_comando == 'insere':
            print('[NÓ %s] - Recebido pedido de inserção' % (self.id_do_no))
            index_msg = msg_filtrada.index(' - ')
            self.insere(msg_filtrada[:index_msg], msg_filtrada[index_msg + 3:], mensagem)
        return


    def Realiza_pedido_cliente(self, Socket_cliente, addr):
        """ Realiza o pedido do cliente em questão
        """
        msg_pedido = Socket_cliente.recv(8192)
        if not msg_pedido:
            Socket_cliente.close()
            return
        mensagem = str(msg_pedido, encoding=self.encoding)
        self.Verifica_Comando(mensagem)


    def Inicializa_No(self):
        """ Inicializa o Nó em questão 
        """
        print('[NÓ %s] - Inicializa o Nó' % (self.id_do_no))
        no_socket = self.Inicia_Socket()  # Cria o socket do nó
        self.lista_select.append(no_socket)  # Insere o socket na lista de select
        print('[NÓ %s] - Nó criado' % (self.id_do_no))

        while True:
            leitura, escrita, excecao = select.select(self.lista_select, [], [])  # listas do select

            # Loop por cada obj que necessita leitura
            for leitura_input in leitura:
                if leitura_input == no_socket:
                    cliente_socket, fim = no_socket.accept()
                    Thread_cliente = threading.Thread(target=self.Realiza_pedido_cliente, args=(cliente_socket, fim))
                    Thread_cliente.start()


    # Função que inicializa a classe
    __No_Inicio = Inicializa_No
