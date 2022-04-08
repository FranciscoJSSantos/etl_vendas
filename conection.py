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


BASE = declarative_base()

def connect_db():
  print("Abrindo conexão com o banco!")
  DIALECT = 'oracle'
  SQL_DRIVER = 'cx_oracle'
  USERNAME = 'vendas' #enter your username
  PASSWORD = '12345678' #enter your password
  HOST = 'oracle-74472-0.cloudclusters.net' #enter the oracle db host url
  PORT = 12498 # enter the oracle port number
  SERVICE = 'XE' # enter the oracle db service name
  ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

  engine = sa.create_engine(ENGINE_PATH_WIN_AUTH)
  return engine