import pandas as pd
import csv


##========== EXTRACT ==========##
#Função para extrair os dados do arquivo original e copiar em um novo
def extrair_dados(arquivo):
    dados = []

    with open(arquivo, mode='r', encoding='utf-8') as arquivo:
        infos = csv.DictReader(arquivo)

        for linha in infos:
            dados.append(linha)

    return(dados)

#Bloco de teste
'''dados = extrair_dados('dataset_corrida_rua.csv')
for linha in dados[:30]:
    print(linha)'''


##========== TRANSFORM ==========##
#Função para transformar e padronizar os dados
def transforma_dados(dados):
    dados_tratados = []

    for linha in dados:
        
        #Trata o ID corredor
        id_corredor = linha['id_corredor']

        try:
            id_corredor = int(linha['id_corredor'])
        except:
            id_corredor = None

        #Trata a idade
        idade = linha['idade']

        try:
            idade = int(linha['idade'])
        except:
            idade = None

        #Trata o gênero
        genero = linha['genero'].strip().lower()

        if genero != '':
            if genero[0] == 'f':
                genero = 'Feminino'
            elif genero[0] == 'm':
                genero = 'Masculino'
            else:
                genero = None
        else:
            genero = None


        #Trata a distancia
        distancia = linha['distancia'].lower().strip()
        
        if distancia != '':
            numero = ''

            for caracter in distancia:
                if caracter.isdigit():
                    numero += caracter #Acumula os caracteres na variável
            
            numero = int(numero)
            distancia_final = f'{numero}km'
        
        elif distancia == '':
            distancia_final =  None

        else:
            distancia_final = None

        #Trata a hora e padroniza no formato HH:MM:SS
        tempo_prova = linha['tempo_prova'].replace('h', ' ').replace('m', ' ').replace('s', ' ').replace(':', ' ').split()

        quantidade = len(tempo_prova)

        if quantidade == 3:
            hora = int(tempo_prova[0])
            minuto = int(tempo_prova[1])
            segundo = int(tempo_prova[2])

            hora = f'{hora:02d}'
            minuto = f'{minuto:02d}'
            segundo = f'{segundo:02d}'

            tempo_prova_final = f'{hora}:{minuto}:{segundo}'
            
        elif quantidade == 2:
            hora = 00
            minuto = int(tempo_prova[0])
            segundo = int(tempo_prova[1])

            hora = f'{hora:02d}'
            minuto = f'{minuto:02d}'
            segundo = f'{segundo:02d}'
                
            tempo_prova_final = f'{hora}:{minuto}:{segundo}'
            
        else:
            tempo_prova_final = None

        tempo_prova_final = f'{hora}:{minuto}:{segundo}'


        #Trata a cidade
        cidade = linha['cidade'].capitalize()

        if cidade == '':
            cidade = None
        else:
            cidade_format = cidade

        #Trata o estado
        estado = linha['estado'].upper().strip()

        if estado == '':
            estado = None
        else:
            estado_format = estado

        #Trata a data do evento
        data_evento = linha['data_evento'].replace('-', ' ').replace('/', ' ').strip().split()

        partes_data = len(data_evento)

        if partes_data == 3:

            bloco1 = data_evento[0]
            bloco2 = data_evento[1]
            bloco3 = data_evento[2]

            tamanho_primeiro_bloco = len(bloco1)

            bloco1 = int(data_evento[0])
            bloco2 = int(data_evento[1])
            bloco3 = int(data_evento[2])
           
            if tamanho_primeiro_bloco == 4:
                ano = bloco1
                mes = bloco2
                dia = bloco3

                ano = f'{ano:04d}'
                mes = f'{mes:02d}'
                dia = f'{dia:02d}'

                data_evento_format = f'{ano}-{mes}-{dia}'
            
            elif tamanho_primeiro_bloco == 2:
                dia = bloco1
                mes = bloco2
                ano = bloco3

                dia = f'{dia:02d}'
                mes = f'{mes:02d}'
                ano = f'{ano:04d}'

                data_evento_format = f'{ano}-{mes}-{dia}'
            
            else:
                data_evento_format = None

        else:
            data_evento_format = None

        #Cria as novas linhas
        nova_linha = {
            'id_corredor': id_corredor,
            'idade': idade,
            'genero': genero,
            'distancia': distancia_final,
            'tempo_prova': tempo_prova_final,
            'cidade': cidade_format,
            'estado': estado_format,
            'data_evento': data_evento_format
        }

        dados_tratados.append(nova_linha)

    return(dados_tratados)

#Bloco de teste
'''dados_tratados = transforma_dados(dados)
for linha in dados_tratados[:20]:
    print(linha)'''


##========== LOAD ==========##
#Função carregar o dado em um novo arquivo
def salvar_dados(dados_tratados, dados_final):
    colunas = dados_tratados[0].keys()

    with open(dados_final, mode='w', newline='', encoding='utf-8') as arquivo_final:

        escritor_csv = csv.DictWriter(arquivo_final, fieldnames=colunas)

        escritor_csv.writeheader()
        escritor_csv.writerows(dados_tratados)

##========= ORQUESTRAÇÃO DO PIPELINE =========##

def main():

    #Arquivos de entrada e saida
    arquivo_entrada = 'arquivos/dataset_corrida_rua.csv'
    arquivo_saida = 'arquivos/dataset_corrida_rua_limpo.csv'


    #=== EXTRACT ===#
    #Chamo a função que extrai os dados do arquivo bruto
    dados = extrair_dados(arquivo_entrada)

    #=== TRANSFORM ===#
    dados_tratados = transforma_dados(dados)
    print('Dados limpos.')

    #=== LOAD ===#

    salvar_dados(dados_tratados, arquivo_saida)

#Verifica se o código esta sendo executado diretamente e executa
if __name__ == "__main__":
    main()

