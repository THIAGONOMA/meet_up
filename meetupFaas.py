import io
import os
import re
import time
import tempfile
import json
import hashlib
import requests
from google.cloud import storage
from functools import reduce
from operator import add, iadd
from google.cloud import vision
from flask import abort
from flask import (jsonify, make_response,)
from flask_api import status as http_status

#Global Variables
mensageDict, tokenWords, result={}, [], {}

#Dictonary common
pattTime={'COMM1':'( |\.|\:|)( |)','HOUR':'[0-9][0-9]:[0-9][0-9](\:|)([0-9]+|)(\.|)([0-9]+|)',
          'DATE':'[0-9][0-9]\/[0-9][0-9](\/([0-9][0-9]|[0-9][0-9][0-9][0-9])|)( |)','DATEBASE':'(DATA|DAT)','DBHOUR':'( |)(HORA|HR|H)',
          'PER':'(PERMAN(Ê|E)NCIA|PERM|PERMA|PERMANEN)','ENT':'(ENTRADA|ENT|EN)','SAI':'(SAIDA|SAI)'}

#Response method
def response_json(message, status='OK', code=http_status.HTTP_200_OK):
    response = {
        'status': status,
        'message': message,
    }

    return make_response(jsonify(response), code)

#Vision API request (in this case - just URI) - transform img in txt
def getVisionFile(file):
    try:
        image = vision.types.Image() #type
        image.source.image_uri = file #path
        client = vision.ImageAnnotatorClient() #instace of object 
        response = client.document_text_detection(image=image) #request
        labels = response.text_annotations #response
        return labels
    except Exception as e: return f'Exception getVisionFile - {str(e)}'
    
#Capture of msg and yours vertices on image, and a simple toknize
def getMsgsAndBounds(labels,_hash,msgPoint=1):
    global mensageDict, tokenWords
    ret,contWord='OK',0
    try:
        for i, text in enumerate(labels):
            #msg 
            if i == 0: mensageDict[f'{_hash} | {msgPoint}']=text.description
            else: 
                vertices = (['({},{})'.format(vertex.x, vertex.y) for vertex in text.bounding_poly.vertices])
                #tokenword
                tokenWords.append((str(_hash), msgPoint, contWord, text.description, vertices))     
                contWord+=1
        return ret
    except Exception as e: return f'Exception - {str(e)}'

#Put a hash in each transaction    
def builderBaseTxt(path):
    try:
        _hash=hashlib.md5(str(time.time()).encode('utf-8')).hexdigest()
        return getMsgsAndBounds(getVisionFile(path),_hash)
    except Exception as e: return f'Exception - {str(e)}'

#Get a time of text   
def getTime(value):
    global pattTime
    res=[]
    try:
        for i, el in enumerate(['ENT','SAI','PER','DATEBASE']):
            case, rest=f'{pattTime[el]}{pattTime["COMM1"]}', None
            if i==3:find=f'{case}{pattTime["DATE"]}{pattTime["DBHOUR"]}{pattTime["COMM1"]}( |){pattTime["HOUR"]}'
            elif i==2:find=f'{case}{pattTime["HOUR"]}'
            else: find=f'{case}{pattTime["DATE"]}{pattTime["HOUR"]}' 
            if re.search(case,value):
                t=[re.sub(pattTime["DBHOUR"],'',re.sub(case,'',matE.group(0).strip().upper())) for matE in re.finditer(find,value)]
                if len(t)>0: rest=t[0] 
            res.append(rest)
        return res
    except Exception as e: return f'Exception - {str(e)}'

#Get a Money   
def getValor(value):
    comm=r'(VALOR|VAL|PAGO|PG|PAG)(\.|\:|)( |)(PAGO|PG|PAG)(\.|\:|)'
    if re.search(comm,value.upper()): return [re.sub(comm,'',matE.group(0).strip().upper()) for matE in re.finditer(f'{comm}( |)(R$|)( |)([0-9]+)(\,|\.|)([0-9]+)(\,|\.|)([0-9]+)',value)][0]
    return None
    
#Constructing Issuer Identity 
def builderIdentit(dat,result):
    #calling API
    masterAct, CNAE=str(dat.get('atividade_principal')[0].get('text')).upper(), reduce(add,[x.group(0).strip() for x in  re.finditer(r'[0-9]',dat.get('atividade_principal')[0].get('code'))])
    name, fantasia=str(dat.get('nome')).upper(), str(dat.get('fantasia')).upper()
    response=requests.get(f'https://servicodados.ibge.gov.br/api/v2/cnae/classes/{CNAE[:5]}')
    response=response.json()
    obsAct=response.get('observacoes')
    #Object constructor
    result['NAME'],result['FANTAS'],result['TYPEACT'],result['OBSACT']=name,fantasia,masterAct,obsAct
    result['UF'],result['MUN'],result['BAIRRO'],result['LOGRA']=dat.get('uf'),dat.get('municipio'),dat.get('bairro'),dat.get('logradouro')
    result['NUM'],result['COMPLE'],result['CEP']=dat.get('numero'),dat.get('complemento'),dat.get('cep')
    return result
    
# Get a registration data
def getOrigin(cnpj):
    ret=reduce(add,[x.group(0).strip() for x in  re.finditer(r'[0-9]',cnpj)])
    response = requests.get(f'https://www.receitaws.com.br/v1/cnpj/{ret}')
    dat=response.json()
    return dat

#Join results for response
def builderResult(token):
    ident={}
    try:
        for _, value in mensageDict.items():
            if re.search(r'(CNPJ|(C|c)npj)(|\.|\:|\-)',value):
                ident['CNPJ']=[re.sub(r'(CNPJ|(C|c)npj)(|\.|\:|\-)','',matE.group(0).strip().upper()) for matE in re.finditer(r'(CNPJ|(C|c)npj)(|\.|\:|\-).+',value)][0]
                dat=getOrigin(ident['CNPJ'])
                result['ESTAB']=builderIdentit(dat,ident)
                result['ENT'],result['SAI'], result['PERM'], result['DATATRANS']=getTime(value)
                result['VALOR']=getValor(value)
            if token: result['tokenwords']=tokenWords
        return result
    except Exception as e: return f'Exception - {str(e)}'
    
#boot function
def pipeLineRasAutomation(request):
    global mensageDict
    try:
        if request.method == 'PUT': return abort(403)
        if request.method != 'GET': return abort(405)
        req = request.args
        try: token = True if len(req)>1 and len(req['tokenwords'])>0 else False
        except Exception as e: token = False     
        eng=builderBaseTxt(req['path'])
        if eng=='OK':
            status, msg = 'OK', builderResult(token)
            mensageDict={}
            return response_json(msg, status)
        else: return response_json('ERROR IN RESULT | '+str(eng), 'NOK')
    except Exception as e: return response_json('ERROR | '+str(e),'NOK')