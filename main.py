import collections
from doctest import testfile
from itertools import count
import re
import string
from traceback import print_tb
from turtle import home
import turtle
from unittest import result
from xml.dom.minidom import Notation
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import delete
from sqlalchemy import text
import sqlalchemy as sa
from time import perf_counter
import time
from datetime import date, datetime, timedelta
import timeit
from Entidades import cliente,fornecedores,itensNota,itensPedido,notaFiscais,parcelas,pedidos,produtos,dmCliente,dmFornecedores,dmProdutos,dmTempo,dmTipoVendas,ftImpontualidades,ftVendas
from Entidades.tempo import Tempo
from conection import connect_db

engine = connect_db()
metadata = sa.MetaData(bind=None)

#Tabelas
table_clientes = sa.Table('CLIENTES', metadata, autoload=True, autoload_with=engine)
table_fornecedores = sa.Table('FORNECEDORES', metadata, autoload=True, autoload_with=engine)
table_itensNota = sa.Table('ITENS_DE_NOTA', metadata, autoload=True, autoload_with=engine)
table_itensPedido = sa.Table('ITENS_DE_PEDIDO', metadata, autoload=True, autoload_with=engine)
table_notasFiscais = sa.Table('NOTAS_FISCAIS', metadata, autoload=True, autoload_with=engine)
table_parcelas = sa.Table('PARCELAS', metadata, autoload=True, autoload_with=engine)
table_pedidos = sa.Table('PEDIDOS', metadata, autoload=True, autoload_with=engine)
table_produtos = sa.Table('PRODUTOS', metadata, autoload=True, autoload_with=engine)

dm_clientes= sa.Table('DM_CLIENTES', metadata, autoload=True, autoload_with=engine)
dm_fornecedores = sa.Table('DM_FORNECEDORES', metadata, autoload=True, autoload_with=engine)
dm_produtos = sa.Table('DM_PRODUTOS', metadata, autoload=True, autoload_with=engine)
dm_tempo = sa.Table('DM_TEMPO', metadata, autoload=True, autoload_with=engine)
dm_tipos_vendas = sa.Table('DM_TIPOS_VENDAS', metadata, autoload=True, autoload_with=engine)
ft_impontualidade = sa.Table('FT_IMPONTUALIDADE', metadata, autoload=True, autoload_with=engine)
ft_vendas = sa.Table('FT_VENDAS', metadata, autoload=True, autoload_with=engine)

def Exclude():
    start = timeit.default_timer()  
    print("Iniciando limpeza da base de dados!")
    engine.execute(text('DELETE ft_vendas'))
    engine.execute(text('DELETE ft_impontualidade'))
    engine.execute(text('DELETE dm_clientes'))
    engine.execute(text('DELETE dm_fornecedores'))
    engine.execute(text('DELETE dm_produtos'))
    engine.execute(text('DELETE dm_tempo'))
    engine.execute(text('DELETE dm_tipos_vendas'))
    end = timeit.default_timer()
    r = (end - start)
    print(f"Tempo total da execução: {r} segundos")

#EXTRAIR 

def ExtractCliente():
    cli = []
    count = 0
    print("Iniciando extração de Cliente")
    start = timeit.default_timer()

    stmt = sa.select([table_clientes])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        cli.append(cliente.Clietes(row[0],row[1],row[2],row[3],row[4]))
        count += 1
    

    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Clientes")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return cli

def ExtractFornecedores():
    forn = []
    count = 0
    print("Iniciando extração de Fornecedores")
    start = timeit.default_timer()

    stmt = sa.select([table_fornecedores])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        forn.append(fornecedores.Fornecedores(row[0],row[1],row[2],row[3]))
        count += 1
    

    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Fornecedores")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return forn

def ExtractItensNotas():
    inot = []
    count = 0
    print("Iniciando extração dos Itens Notas")
    start = timeit.default_timer()

    stmt = sa.select([table_itensNota])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        inot.append(itensNota.ItensNota(row[0],row[1],row[2],row[3]))
        count += 1
    
    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Itens notas")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return inot

def ExtractItensPedido():
    iped = []
    count = 0
    print("Iniciando extração dos Itens pedido")
    start = timeit.default_timer()

    stmt = sa.select([table_itensPedido])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        iped.append(itensNota.ItensNota(row[0],row[1],row[2],row[3]))
        count += 1
        
    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Itens pedido")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return iped

def ExtractNotasFiscais():
    nf = []
    count = 0
    print("Iniciando extração das Notas ficais")
    start = timeit.default_timer()

    stmt = sa.select([table_notasFiscais])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        nf.append(notaFiscais.NotasFiscais(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9]))
        count += 1

    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção das Notas ficais")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return nf

def ExtractParcelas():
    parc = []
    count = 0
    print("Iniciando extração das Parcelas")
    start = timeit.default_timer()

    stmt = sa.select([table_parcelas])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        parc.append(parcelas.Parcelas(row[0],row[1],row[2],row[3]))
        count += 1
    
    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção das Parcelas")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return parc

def ExtractPedidos():
    ped = []
    count = 0
    print("Iniciando extração dos Pedidos")
    start = timeit.default_timer()

    stmt = sa.select([table_pedidos])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        ped.append(pedidos.Pedidos(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
        count += 1

    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Pedidos")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return ped

def ExtractProdutos():
    prod = []
    count = 0
    print("Iniciando extração dos Prodtos")
    start = timeit.default_timer()

    stmt = sa.select([table_produtos])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        prod.append(produtos.Produtos(row[0],row[1],row[2],row[3],row[4],row[5]))
        count += 1
    
    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Produtos")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return prod

def ExtractTempo():
    temp = []
    count = 0
    print("Iniciando extração dos Tempos")
    start = timeit.default_timer()

    stmt = sa.select([table_notasFiscais])
    result = engine.execute(stmt).fetchall()
            
    for row in result:
        temp.append(Tempo(row[7]))
        count += 1

    end = timeit.default_timer()
    r = (end - start)
    print("Fim da extrção dos Tempos")
    print(f'Total de itens exraidos: {count}')
    print(f"Tempo total da execução: {r} segundos")
    
    return temp

###TRANSFORMAÇÃO 

def TransformarCliente():
    clientesDW = []
    print("Iniciando processo de transformação de Clientes")
    start = timeit.default_timer()
    cli = ExtractCliente()
    
        
    for i in cli:
       clientesDW.append(dmCliente.DMCliente(i.cod_cli,i.nom_cli,"Aracaju","SE"))
    
        
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação dos Clientes. "
          f"- Tempo de transformação: {r} segundos")
    
    return clientesDW

def TransformarFornecedor():
    fornecedoresDW = []
    c = 0
    print("Iniciando processo de transformação de Fornecedores")
    start = timeit.default_timer()
    forn = ExtractFornecedores()
    
        
    for i in forn:
       c+=1
       fornecedoresDW.append(dmFornecedores.DMFornecedores(c,i.nom_forn,i.uf_forn))
     
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação dos Fornecedores. "
          f"- Tempo de transformação: {r} segundos")
    
    return fornecedoresDW

def TransformarProdutos():
    produtosDW = []
    print("Iniciando processo de transformação de Produtos")
    start = timeit.default_timer()
    produtos = ExtractProdutos()
    
        
    for i in produtos:
       produtosDW.append(dmProdutos.DMProdutos(i.cod_prod,i.dsc_prod,"Teste"))
    
        
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação dos Produtos. "
          f"- Tempo de transformação: {r} segundos")
    
    return produtosDW

def TransformarTempo():
    tempoDW = []
    count = 0
    print("Iniciando processo de transformação de Tempo")
    start = timeit.default_timer()
    tempo = ExtractTempo()
    
        
    for i in tempo:
       count+=1
       tempoDW.append(dmTempo.DMTempo(count,i.dat_nota.strftime("%Y"),int(i.dat_nota.strftime("%m")),int(i.dat_nota.strftime("%Y")),i.dat_nota.strftime("%b"),NomMes(int(i.dat_nota.strftime("%m")))[0:3],'0000',23))
    
        
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação dos Tempo. "
          f"- Tempo de transformação: {r} segundos")
    
    return tempoDW

def Turn(hora):
    turno = "Teste"
    h = int(hora)
    if(h < 12):
        turno = "MANHA"
    elif(h > 12 and h <= 18):
        turno = "TARDE"
    elif(h > 18):
        turno = "NOITE"
    return turno

def NomMes(num):
    nome = "Teste"
    if(num == "01"):
        nome = "Janeiro"
    elif(num == "02"):
        nome = "Fevereiro"
    elif(num == "03"):
        nome = "Março"
    elif(num == "04"):
        nome = "Abril"
    elif(num == "05"):
        nome = "Maio"
    elif(num == "06"):
        nome = "Junho"
    elif(num == "07"):
        nome = "Julho"
    elif(num == "08"):
        nome = "Agosto"
    elif(num == "09"):
        nome = "Setemebro"
    elif(num == "10"):
        nome = "Outubro"
    elif(num == "11"):
        nome = "Novembro"
    elif(num == "12"):
        nome = "Dezembro"
    
    return nome

def TransformarTipoVendas():
    tipoVendasDW = []
    cnt = 0
    print("Iniciando processo de transformação de Produtos")
    start = timeit.default_timer()
    produtos = ExtractPedidos()
    
        
    for i in produtos:
        cnt+=1
        tipoVendasDW.append(dmTipoVendas.DMTiposVendas(cnt,"a vista" if(i.val_a_prazo == 0) else ("a prazo")))
      
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação dos Tipos de vendas. "
          f"- Tempo de transformação: {r} segundos")
    
    return tipoVendasDW

def TransformarImpontualidade():
    impotualidadeDW = []
    c = 0
    print("Iniciando processo de transformação de Impontualidade")
    start = timeit.default_timer()
    tempo = ExtractTempo()
    
        
    for i in tempo:
       c+=1
       impotualidadeDW.append(ftImpontualidades.FTImpontualidades(c,c,7,8))
    

    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação de Impontualidade. "
          f"- Tempo de transformação: {r} segundos")
    
    return impotualidadeDW

def TransformarVendas():
    vendasDW = []
    c = 0
    print("Iniciando processo de transformação de Vendas")
    start = timeit.default_timer()
    prod = ExtractProdutos()
    
        
    for i in prod:
       c+=1
       vendasDW.append(ftVendas.FTVendas(i.cod_prod,c,c,199))

    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de transformação de Vendas. "
          f"- Tempo de transformação: {r} segundos")
    
    return vendasDW

###CARREGAR

def CarregarDmCliente():
    print("Iniciando processo de Carregamento dos Clientes")
    start = timeit.default_timer()
    cli = TransformarCliente()
        
    for item in cli :
        ins = dm_clientes.insert().values(id_cliente = item.id_cliente, nome_cliente = item.nome_cliente, cidade_cli = item.cidade_cli, uf_cli = item.uf_cli)
        result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento dos Clientes. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarDmFornecedores():
    print("Iniciando processo de Carregamento dos Fornecedores")
    start = timeit.default_timer()
    forn = TransformarFornecedor()
        
    for item in forn :
        ins = dm_fornecedores.insert().values(id_forn = item.id_forn ,nom_forn = item.nom_forn ,regiao_forn = item.regiao_forn )
        result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento dos Fornecedores. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarDmProdutos():
    print("Iniciando processo de Carregamento dos Produtos")
    start = timeit.default_timer()
    prod = TransformarProdutos()
        
    for item in prod :
        ins = dm_produtos.insert().values(id_prod = item.id_prod, dsc_prod = item.dsc_prod, classe_prod = item.classe_prod )
        result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento dos Fornecedores. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarDmTempo():
    print("Iniciando processo de Carregamento dos Produtos")
    start = timeit.default_timer()
    tempo = TransformarTempo()
        
    for item in tempo :
        ins = dm_tempo.insert().values(id_tempo = item.id_tempo ,nu_ano = item.nu_ano ,nu_mes= item.nu_mes,nu_anomes = item.nu_anomes,sg_mes = item.sg_mes ,nm_mesano = item.nm_mesano ,nm_mes = item.nm_mes, nu_dia = item.nu_dia)
        result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento dos Fornecedores. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarDmTipoVendas():
    print("Iniciando processo de Carregamento dos Tipos de vendas")
    start = timeit.default_timer()
    mov = TransformarTipoVendas()
        
    for item in mov :
        ins = dm_tipos_vendas.insert().values(id_tipo_venda = item.id_tipo_venda, desc_tipo_venda = item.desc_tipo_venda)
        result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento dos Tipos de vendas. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarFTImpontualidade():
    print("Iniciando processo de Carregamento das Impontualidades")
    start = timeit.default_timer()
    temp = TransformarTempo() 
    cli = TransformarCliente()
        
    for cliente in cli:     
        for item in temp :
            ins = ft_impontualidade.insert().values(id_tempo = item.id_tempo, id_cliente = cliente.id_cliente, valor_parc_atrasadas = 156.9, valor_parc_total = 78.6 )
            result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento das Impontualidades. "
          f"- Tempo de transformação: {r} segundos")
    
def CarregarFTVendas():
    c = 0
    print("Iniciando processo de Carregamento das Vendas")
    start = timeit.default_timer()
    temp = TransformarTempo() 
    cli = TransformarProdutos()
    tpvenda = TransformarTipoVendas()
        
    for produto in cli:
        c+=1     
        for item in temp :
            for tv in tpvenda:
                ins = ft_vendas.insert().values(id_prod = produto.id_prod, id_tempo = item.id_tempo, id_tipo_venda =  tv.id_tipo_venda, id_forn = c, valor_venda = 10 )
                result = engine.execute(ins)
                
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado processo de Carregamento das Vendas. "
          f"- Tempo de transformação: {r} segundos")

def ETL():
    print("Iniciando rotina ETL")
    start = timeit.default_timer()
    CarregarDmCliente()
    CarregarDmFornecedores()
    CarregarDmProdutos()
    CarregarDmTempo()
    CarregarDmTipoVendas()
    CarregarFTImpontualidade()
    CarregarFTVendas()
    end = timeit.default_timer()
    r = (end - start)
    print("Finalizado a rotina ETL. "
          f"- Tempo de processamente: {r} segundos")

