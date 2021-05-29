from random import randint
import select
import socket
import uuid
import sys

# Configurações do Cliente
HOST = 'localhost'
PORT = 5000

# Variaveis de ambiente necessarias
encoding = "UTF-8"
numero_no = 0
id_cliente = uuid.uuid4()
ativo = True

# Configurações do Socket
sock = socket.socket()
sock.connect((HOST, PORT))
sock.settimeout(4)


def Envia_Recebe(socket_obj, msg_inicial, tamanho):
    """ Envia e aguarda o recebimento
    """
    socket_obj.send(msg_inicial.encode(encoding))

    try:
        Mensagem_Recebida = socket_obj.recv(tamanho)
        mensagem = str(Mensagem_Recebida, encoding=encoding)
        return mensagem
    except:
        return


def Instrucoes():
    """ Imprime a lista de comandos para o usuário
    """
    print("Para sabe os possiveis comandos, digite [ajuda]:")
    print("Para inserir o par chave/valor na tabela, digite [inserir]:")
    print("Para buscar o valor referente a uma chave, digite [busca]:")
    print("Para encerrar o cliente, digite [parar]:")


def Escolha_Acao(entrada_cliente):
    """ Verifica se o cliente digitou algum comando
    """
    global ativo, numero_no, id_cliente

    if entrada_cliente == "parar":
        ativo = False

    if entrada_cliente == "ajuda":
        Instrucoes()

    elif entrada_cliente == "inserir":
        no_requisitado = randint(0, numero_no-1)
        chave = input('Insira a Chave: ')
        valor = input('Insira o Valor: ')
        insere(no_requisitado, chave, valor)

    elif entrada_cliente == "busca":
        no_requisitado = randint(0, numero_no-1)
        chave = input('Insira a Chave: ')
        porta = 5000 + numero_no + randint(1, 10000)
        busca(porta, no_requisitado, chave)

    else:
        print('Comndo inexistente, digite [ajuda] e confira as opções')


def get_endereco_no(no_de_origem):
    """ Função para solicitar o endereço do nó desejado
    """
    global sock
    anexo = "((get_endereco_no))"
    var_aux =  anexo + str(no_de_origem)
    mensagem = Envia_Recebe(sock, var_aux, 1024)

    # Filtra a mensagem
    comeco_anexo = mensagem.index('((')
    fim_anexo = mensagem.index('))')
    instrucao = mensagem[comeco_anexo + 2:fim_anexo]
    porta = mensagem[fim_anexo + 2:]

    if instrucao == 'endereco':
        return int(porta)


def Conecta_No(no_de_origem, mensagem):
    """ Função de conecção com um nó para poder fazer uma requisição
    """
    porta = get_endereco_no(no_de_origem)
    sock = socket.socket()
    sock.connect((HOST, porta))
    sock.settimeout(4)

    sock.send(mensagem.encode(encoding))
    
    sock.close()
    return


def insere(no_pai, chave, valor):
    """ Função de inserção de um par chave/valor no Chord
    """
    anexo = "((insere))"
    msg = anexo +  '%s - %s' % (chave, valor)
    Conecta_No(no_pai, msg)
    return


def Espera_Resposta_No(porta):
    """ Organiza os sockets liberando e bloqueando de acordo com a entrada e saida de clientes
    """
    socket_de_espera = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_de_espera.bind((HOST, porta))
    socket_de_espera.listen(16)  # espera por até 2^n conexões
    socket_de_espera.setblocking(False)  # torna o socket não bloqueante na espera por conexões
    return socket_de_espera, porta


def busca(id_buscado, no_pai, chave):
    """ Função de busca de uma chave no Chord
    """
    cliente_sock, cliente_porta = Espera_Resposta_No(id_buscado)
    anexo = "((busca))"
    req = anexo  + '%s - %s' % (str(cliente_porta), chave)
    Conecta_No(no_pai, req)

    leitura, escrita, excecao = select.select([cliente_sock], [], [])
    for leitura_input in leitura:
        # significa que a leitura recebeu pedido de conexão
        if leitura_input == cliente_sock:
            novo_socket, endereco = cliente_sock.accept()
            msg = novo_socket.recv(8192)
            mensagem = msg.decode(encoding)

            # Filtra de mensagem
            comeco_anexo = mensagem.index('((')
            fim_anexo = mensagem.index('))')
            anexo_msg = mensagem[comeco_anexo + 2:fim_anexo]
            info_msg = mensagem[fim_anexo + 2:]

            if anexo_msg == 'FEITO':
                index_anexo = info_msg.index(' - ')
                no = info_msg[:index_anexo]
                val = info_msg[index_anexo+3:]
                print("Chave encontrada no Nó %s {%s: %s}" % (no, chave, val))
            else:
                print("Chave não encontrada")
    return


def main():
    """ Função main que inicializa as Threads
    """
    global ativo, numero_no
    print("--------- CLIENTE ---------")
    Instrucoes()

    # Solicitação do número N para definir o total de nós
    anexo = "((init_cliente))"

    mensagem = Envia_Recebe(sock, anexo, 1024)
    
    # Filra mensagem
    comeco_anexo = mensagem.index('((')
    fim_anexo = mensagem.index('))')
    instrucao = mensagem[comeco_anexo + 2:fim_anexo]
    num = mensagem[fim_anexo + 2:]

    if instrucao == 'N':
        numero_no = int(num)

    while ativo:
        comando = input('Qual da opções vocês desejam escolher? ')
        if comando in [" ", "\\n", ""]:
            print("Comando inválido, insira uma opção valida.")
            continue
        Escolha_Acao(comando)

    print("Encerrando cliente e finalizando a conexão.")
    sock.close()
    sys.exit()


main()
