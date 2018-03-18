# -*- coding: utf-8 -*-
"""
Created on Sat Mar 17 22:31:00 2018

@author: pedro
"""

import zeep
import zipfile
from bs4 import BeautifulSoup
import requests, io
import xmltodict, json
import datetime 
import pandas as pd

#
#def OpenXML_UNICO(URL):
#    r = requests.get(URL, stream=True)
#    zf = zipfile.ZipFile(io.BytesIO(r.content))
#    x=zf.namelist()[0]
#    soup = BeautifulSoup(str(zf.read(x).decode('UTF-8')), 'html.parser')
#    return(soup)

#def OpenXML_JSON(URL):
#    r = requests.get(URL, stream=True)
#    zf = zipfile.ZipFile(io.BytesIO(r.content))
#    x=zf.namelist()[0]
#    o = xmltodict.parse(str(zf.read(x).decode('UTF-8')))
#    o=json.loads(json.dumps(o))
#    return(o)
#

def save(URL,name):
    r = requests.get(URL, stream=True)
    #save
    with open("./"+str(name)+".zip", "wb") as code:
        code.write(r.content)
    print("Saved")



##Web method utilizado pelo sistema cliente para verificação de quais arquivos de competências sofreram atualizações, oriundas de entregas de documentos (pelos administradores) feitas a partir da data passada como parâmetro.

def LoginCVM(wsdl,lg,pw):
    ## logar no servidor retornando conecxao e autenticacoa. 
    client = zeep.Client(wsdl=wsdl)
    response = client.service.Login(lg, pw)
    response_header = response["header"]     
    return(response_header,client)

#---------------------Funcoes de dados primordiais dos fundos ----------------------------------------------------------
##                  lista de fundos.
def solicAutorizDownloadCadastroCVM(response_header,client,data):
    # -- Retorna arquivo com dados diarios de fundos cadastrados. 
    result_func = client.service.solicAutorizDownloadCadastro(
                        _soapheaders=[response_header],
                        strDtRefer=data,
                        strMotivoAutorizDownload="Motivos de estudo", 
                    )
    return(result_func.body.solicAutorizDownloadCadastroResult)

##------- Dados de informacoes diarias, atualizacao.
def solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo):
    ## --- captura diariamente as informacoes publicas.
    num= 50 if 'b' in arquivo else 209
    result_func = client.service.solicAutorizDownloadArqEntregaPorData(
                        _soapheaders=[response_header],
    #                    iCdTpDoc = 209,
                        iCdTpDoc = num,
                        strDtEntregaDoc=data,
                        strMotivoAutorizDownload="Motivos de estudo", 
                    )
    return(result_func.body.solicAutorizDownloadArqEntregaPorDataResult)

### -------------------------   TESTES
### ----------------------- teste otimizacao 
# -------------            cadastro de fundos 
holidays = requests.get('https://raw.githubusercontent.com/wilsonfreitas/python-bizdays/master/ANBIMA.txt').text.split("\n")

wsdl = 'http://sistemas.cvm.gov.br/webservices/Sistemas/SCW/CDocs/WsDownloadInfs.asmx?WSDL'
lg='1906'
pw='4590'

'''
    * Rene:
        -Implementar lista de loging o objeto "Login_list" como lista.
        6800 dias aproximadamente. Podemos fazer 3 dias por login:
                6800/3=2267 (numero de loguins para fazer em um dia.)
                453 para fazer em 4 dias.
'''
Login_list=[]

i=0
l=0

for data in [((datetime.date.today() - datetime.timedelta(days=x)).strftime('%Y-%m-%d')) for x in range(6800) if (5!= (datetime.date.today() - datetime.timedelta(days=x)).weekday() != 6)]:
    if sum(data==pd.Series(holidays))==0:
        if 1+i%4==0:
            l+=l
    
        lg=Login_list[l][0]
        pw=Login_list[l][1]
        
        response_header,client=LoginCVM(wsdl,lg,pw)
        result_func=solicAutorizDownloadCadastroCVM(response_header,client,data)
    
        ## json formater, cadastro
        URL=result_func
        name=str(data)+"-Cadastro"
        save(URL,name)
        
        #----------------- Dados diarios.
        #response_header,client=LoginCVM(wsdl,lg,pw)
        #data="2017-08-18"
        arquivo='d'
        result_func=solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo)
    
        URL=result_func
        name=str(data)+"-Diario"
        save(URL,name)
        
        arquivo='b'
        result_func=solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo)
    
        URL=result_func
        name=str(data)+"-Balan"
        save(URL,name)
        i+=i
    
