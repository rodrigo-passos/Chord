from hashlib import sha1
import instancia_no_chord
import threading
import socket
import select

# Configurações do Servidor
HOST = ''
PORT = 5000
encoding = "UTF-8"

# Variaveis de ambiente necessarias
entradas = []  # define entrada padrão
dict_conexoes = {}  # armazena historico de dict_conexoes
dict_id_endereco = {}  # associa um id único a um endereço (conexão de cliente ip + porta)
list_nos_chord = [] # Lista com os nós chord
n_nos_entrada = 0 # Numero dos nós de entrada
dict_enderecos_nos = {}  # dicionario de id do chord + porta
instancias = [] # Lista com as instancias


def Inicia_Servidor():
    """ Inicia o servidor e adiciona o socket do servidor nas entradas
    """
    global n_nos_entrada
    n_nos_entrada = 4 
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST, PORT))
    sock.listen(16)  # Escuta os nós
    sock.setblocking(False)  # torna o socket desbloqueado e espera por conexões
    entradas.append(sock) # Adiciona na lista de entrada

    return sock


def Inicia_Anel():
    """ Inicializa o Anel Chord na função principal
    """
    global n_nos_entrada, list_nos_chord, PORT, dict_enderecos_nos, instancias
    
    for i in range(16): 

        dict_enderecos_nos[i] = PORT + i + 1

        prox_no = []
        for j in range(4): 
            pos = (i + 2 ** j) % 16 
            prox_no.append(PORT + 1 + pos)

        # cria e inicia nova thread para gerar os nós
        novo_no = threading.Thread(target=instancia_no_chord.No_chord, args=(PORT + i + 1, i, n_nos_entrada, prox_no))
        novo_no.start()

        # armazena a referencia da thread para usar com join()
        list_nos_chord.append(novo_no)

    return


def Novo_Cliente(sock):
    """ Gerencia o recebimento de conexões de clientes, recebe o socket do servidor
    """
    novo_socket, endereco = sock.accept()

    print('[SERVIDOR] - Conectado com o seguinte endereço: ' + str(endereco))

    dict_conexoes[endereco] = novo_socket  # registra a nova conexão no dicionário de conexões
    if len(dict_id_endereco) == 0:
        dict_id_endereco[1] = endereco
    else:
        index = max(dict_id_endereco, key=dict_id_endereco.get) + 1
        dict_id_endereco[index] = endereco

    return novo_socket, endereco


def Instrucoes(sock, instrucao):
    """ Verifica o comando enviado e o executa
    """
    comeco_anexo = instrucao.index('((')
    fim_anexo = instrucao.index('))')
    comando = instrucao[comeco_anexo + 2:fim_anexo]
    info_msg = instrucao[fim_anexo + 2:]

    # Comando de busca do endereço de um nó
    if comando == 'get_endereco_no':
        endereco = dict_enderecos_nos[int(info_msg)]
        anexo_mensagem = "((endereco))"
        resposta = anexo_mensagem + str(endereco)
        sock.send(resposta.encode(encoding))
    # Comando de inicialização do Client para verificar o número total de nós
    elif comando == 'init_cliente':
        anexo_mensagem = "((N))"
        resposta = anexo_mensagem + str(16)
        sock.send(resposta.encode(encoding))


def Faz_Requisicoes(cliente_socket, endereco):
    """ Processa as requisições do cliente
    """
    global encoding, dict_enderecos_nos, n_nos_entrada

    while True:
        msg_cliente = cliente_socket.recv(8192)

        if not msg_cliente:
            print("[SERVIDOR] - " +str(endereco) + ' encerrou a conexão.')

            del dict_conexoes[endereco]
            del dict_id_endereco[list(dict_id_endereco.keys())[list(dict_id_endereco.values()).index(endereco)]]
            cliente_socket.close()
            return

        mensagem = (str(msg_cliente, encoding=encoding))
        Instrucoes(cliente_socket, mensagem)


def main():
    sock = Inicia_Servidor()  # pega o socket do servidor
    Inicia_Anel()

    print("[SERVIDOR] - Esperando por conexões ...")

    while True:
        leitura, escrita, excecao = select.select(entradas, [], [])  # listas do select

        # percorre cada objeto de leitura (conexão socket, entrada de teclado)
        for leitura_input in leitura:
            # significa que a leitura recebeu pedido de conexão
            if leitura_input == sock:
                clientSock, endereco = Novo_Cliente(sock)
                Thread_cliente = threading.Thread(target=Faz_Requisicoes, args=(clientSock, endereco))
                Thread_cliente.start()


# Função Principal
main()
