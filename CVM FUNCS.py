# -*- coding: utf-8 -*-
"""
Created on Sat Aug 19 12:06:57 2017
@author: pedro
Funcoes de WS cvm
"""
import zeep
import zipfile
from bs4 import BeautifulSoup
import requests, io
import xmltodict, json
import pymongo
import datetime
import csv

#
#def OpenXML_UNICO(URL):
#    r = requests.get(URL, stream=True)
#    zf = zipfile.ZipFile(io.BytesIO(r.content))
#    x=zf.namelist()[0]
#    soup = BeautifulSoup(str(zf.read(x).decode('UTF-8')), 'html.parser')
#    return(soup)

def PercorreCSV():
    global logins,lg,pw
    credencial = next(logins)
    lg=credencial[0]
    pw=credencial[1]

def OpenXML_JSON(URL):
    r = requests.get(URL, stream=True)
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    x=zf.namelist()[0]
    o = xmltodict.parse(str(zf.read(x).decode('UTF-8')))
    o=json.loads(json.dumps(o))
    return(o)


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
    try:
        result_func = client.service.solicAutorizDownloadCadastro(
                            _soapheaders=[response_header],
                            strDtRefer=data,
                            strMotivoAutorizDownload="Motivos de estudo", 
                        )
        status = 1
    except Exception as e:
        exce=e.args
        if exce[0] == 'Arquivo para download não encontrado para os parâmetros especificados':
            status = 2
            return(0,status)
        elif exce[0] == 'Conversão do parâmetro strDtRefer para data não retorna dia útil':
            status = 2
            return(0,status)
        elif exce[0] == 'Permissão negada. Por favor, efetue o login antes de acessar essa funcionalidade.':
            status = 0
            return(0,status)
    return(result_func.body.solicAutorizDownloadCadastroResult,status)

##------- Dados de informacoes diarias, atualizacao.
def solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo):
    ## --- captura diariamente as informacoes publicas.
    num= 50 if 'b' in arquivo else 209
    try:
        result_func = client.service.solicAutorizDownloadArqEntregaPorData(
                            _soapheaders=[response_header],
        #                    iCdTpDoc = 209,
                            iCdTpDoc = num,
                            strDtEntregaDoc=data,
                            strMotivoAutorizDownload="Motivos de estudo", 
                        )
        status = 1
    except:
        status = 0
        return(0,status)
    return(result_func.body.solicAutorizDownloadArqEntregaPorDataResult,status)

### ----------------------- BANCO MONGO
# -------------            CONEXÃO COM O BANCO

con = pymongo.MongoClient("mongodb://localhost")

db = con.cvm
cvmdb= db.cvm

### -------------------------   TESTES
### ----------------------- teste otimizacao 
# -------------            cadastro de fundos 
login_file = open('mycsvfileRene.csv', 'r')
logins = csv.reader(login_file)

wsdl = 'http://sistemas.cvm.gov.br/webservices/Sistemas/SCW/CDocs/WsDownloadInfs.asmx?WSDL'
lg=''
pw=''
PercorreCSV()
response_header,client=LoginCVM(wsdl,lg,pw)

for data in [((datetime.date.today() - datetime.timedelta(days=x)).strftime('%Y-%m-%d')) for x in range(2,2500) if (6!= (datetime.date.today() - datetime.timedelta(days=x)).weekday() != 5)]:
    status = 0
    while status > 0:
        result_func,status=solicAutorizDownloadCadastroCVM(response_header,client,data)
        if status == 0:
            print("teste")
            PercorreCSV()
            response_header,client=LoginCVM(wsdl,lg,pw)
    
    if status == 2:
        break
    print(data)
    ## json formater, cadastro
    Jcadas=OpenXML_JSON(result_func)
    Fundos={}
    ## Dados do fundo.
    for c in range(len(Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"])):
       Fundos.update({Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"][c]["CNPJ"].replace(".","").replace("/","").replace("-",""):{"Cadastro":{Jcadas["ROOT"]["CABECALHO"]["DT_REFER"]:Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"][c]}}})
       Fundos[Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"][c]["CNPJ"].replace(".","").replace("/","").replace("-","")].update({"Diario":{},"Balanco":{}})
       
       fundojson= {}
       fundojson={"_id":Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"][c]["CNPJ"].replace(".","").replace("/","").replace("-","")}
       cvmdb.update_one({'_id': fundojson["_id"]}, {'$set':fundojson}, upsert=True)
       
       DT_REF = datetime.datetime.strptime(Jcadas["ROOT"]["CABECALHO"]["DT_REFER"], '%Y-%m-%d')
       
       fundojson.update({"cadastro":[]})   
       fundojson["cadastro"].append({'DT_REFER': DT_REF, 'info': Jcadas["ROOT"]["PARTICIPANTES"]["CADASTRO"][c]})
       
       CursorUpdateInfoPrev = cvmdb.aggregate([
                            {"$unwind": "$cadastro"},
                            {"$match": {"_id": fundojson["_id"], "cadastro.DT_REFER": {"$lte": DT_REF}}},         
                            {"$project": 
                                {"cadastro.DT_REFER": 1, 
                                    "cadastro.info": 
                                    {"$cond": 
                                        {"if": {"$eq": ["$cadastro.info", fundojson['cadastro'][0]['info']]},
                                            "then": 1,
                                            "else": 0
                                        }
                                    }
                                }
                            },
                            {"$sort": {"cadastro.DT_REFER": -1}},
                            {"$limit": 1}
                        ])
    
       aux = 0
       for j in CursorUpdateInfoPrev:
           aux = 1
           if j["cadastro"]["info"] == 0: 
               cvmdb.update_one({'_id': fundojson["_id"]}, {'$push': {'cadastro': fundojson["cadastro"][0]}})
        
       if aux == 0:
           cvmdb.update_one({'_id': fundojson["_id"]}, {'$push': {'cadastro': fundojson["cadastro"][0]}})
               
       CursorUpdateInfoNext = list(cvmdb.aggregate([
                           {"$unwind": "$cadastro"},
                            {"$match": {"_id": fundojson["_id"], "cadastro.DT_REFER": {"$gt": DT_REF}}},         
                            {"$project": 
                                {"cadastro.DT_REFER": 1, 
                                    "cadastro.info": 
                                    {"$cond": 
                                        {"if": {"$eq": ["$cadastro.info", fundojson['cadastro'][0]['info']]},
                                            "then": 1,
                                            "else": 0
                                        }
                                    }
                                }
                            },
                            {"$sort": {"cadastro.DT_REFER": 1}},
                            {"$limit": 1}
                        ]))
        
       for i in CursorUpdateInfoNext:
          if i["cadastro"]["info"] == 1:
             cvmdb.update_one({'_id': fundojson["_id"]}, {'$pull': {'cadastro': {'DT_REFER': i["cadastro"]["DT_REFER"], 'info': fundojson['cadastro'][0]['info']}}})
    
    
    #----------------- Dados diarios.
    arquivo='d'
    
    status = 0
    while status == 0:
        result_func,status=solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo)
        if status == 0:
            PercorreCSV()
            response_header,client=LoginCVM(wsdl,lg,pw)
            
    Jcadas=OpenXML_JSON(result_func)
    # dado diario
    Diario={}
    ## Dados do fundo.
    for c in range(len(Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"])):
        if Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","") in Fundos.keys():
            Diario.update({Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-",""):{Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["DT_COMPTC"]:Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]}})
            Fundos[Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","")]["Diario"].update(Diario[Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","")])
            
        diariojson= {}
        diariojson={"_id":Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-",""), "diario":[]}
        
        DT_REF = datetime.datetime.strptime(Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]["DT_COMPTC"], '%Y-%m-%d')
        
        diariojson["diario"].append({'DT_REFER': DT_REF, 'info': Jcadas["ROOT"]["INFORMES"]["INFORME_DIARIO"][c]})
       
        cvmdb.update_one({'_id': diariojson["_id"]}, {'$push': {'diario': diariojson["diario"][0]}})
    
    #----------------- Dados balanco.
    arquivo='b'
    
    status = 0
    while status == 0:
        result_func=solicAutorizDownloadArqEntregaPorDataCVM(response_header,client,data,arquivo)
        if status == 0:
            PercorreCSV()
            response_header,client=LoginCVM(wsdl,lg,pw)
    
    Jcadas=OpenXML_JSON(result_func)
    
    # dado diario
    Balanco={}
    ## Dados do fundo.
    for c in range(len(Jcadas["ROOT"]["INFORMES"]["BALANCETE"])):
        if Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","") in Fundos.keys():
            Balanco.update({Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-",""):{Jcadas["ROOT"]["INFORMES"]["BALANCETE"][0]["DT_COMPTC"]:{"contas": Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["LISTA_CONTAS"]["CONTA"],"PLANO_CONTABIL":Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["PLANO_CONTABIL"],"TIPO_FDO":Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["TIPO_FDO"],"Dia_publicado":Jcadas["ROOT"]["CABECALHO"]["DT_REFER"]}}})
            Fundos[Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","")]["Balanco"].update(Balanco[Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-","")])
            
        balancojson= {}
        balancojson={"_id":Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["CNPJ_FDO"].replace(".","").replace("/","").replace("-",""), "balanco":[]}
        
        DT_REF = datetime.datetime.strptime(Jcadas["ROOT"]["CABECALHO"]["DT_REFER"], '%Y-%m-%d')
        
        balancojson["balanco"].append({'DT_REFER': DT_REF, 'info': {"PLANO_CONTABIL":Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["PLANO_CONTABIL"],"TIPO_FDO":Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["TIPO_FDO"],"Dia_publicado":Jcadas["ROOT"]["CABECALHO"]["DT_REFER"],"contas": Jcadas["ROOT"]["INFORMES"]["BALANCETE"][c]["LISTA_CONTAS"]["CONTA"]}})
        cvmdb.update_one({'_id': balancojson["_id"]}, {'$push': {'balanco': balancojson["balanco"][0]}})
    
    ### ---------------------- Desenvolver coleta e carga de historico/ 

#Fundos[list(Fundos.keys())[10]]
