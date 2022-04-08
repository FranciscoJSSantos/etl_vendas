from main import ETL, Exclude

while(True):
    print("======================================")
    teste = int(input("Menu de funções: \n1 - Limpar a tabela dimensional \n2 - ETL \n3 - Sair \nInforme a opção desejada: "))
    if(teste == 1):
        Exclude()
    elif (teste == 2):
        ETL()
    else:
        break

    
