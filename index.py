from flask import Flask, Response, jsonify
from flask import request

from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import os
import mysql.connector
import pymongo
from pymongo import MongoClient
import json
from bson import ObjectId
import xmltodict
import xml.etree.ElementTree as ET
from unidecode import unidecode
from datetime import datetime

app = Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    fii_ticker = request.args.get('ticker')

    dy = "NOT FOUND"
    try :
        driver_path = 'C:\\Users\\Isaac\Documents\\python_workspace\\api_fiis\\chromedriver.exe'
        options = ChromeOptions()
        # options.add_argument("--headless")
        driver = Chrome(executable_path=driver_path, options=options)
        driver.set_window_size(1280, 900)
        driver.get(f'https://statusinvest.com.br/fundos-imobiliarios/{fii_ticker}')

        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "dy-info")))

        cardDy = driver.find_element_by_id('dy-info')
        dy = cardDy.find_element(By.XPATH, "//strong[@class='value d-inline-block fs-5 fw-900']").text
        dy = dy.replace(",", ".").strip()
        driver.quit()
    except Exception as e:
        print('erro ao buscar o dado na página ', str(e))
    finally:
        return f"<value>{dy}</value>"

def conectar_banco():
    return mysql.connector.connect(
        user='root',
        password='123456',
        host='192.168.0.190',
        database='fii'
    )

def insertMongoDB(json_list):
    print('====== GUARDAR DADOS NO BANCO DE DADOS MONGO ==========================')
    # print(json_list)
    client = MongoClient("mongodb://192.168.0.190:27017")
    db = client.fiis
    collection = db.fiis

    # verificar se o ticker ja existe e atualizar os valores 
    for fii in json_list:
        # filtro = {"FUNDOS" : fii['FUNDOS']}
        # # print(filtro)
        # registro = collection.find_one(filtro)
        # if registro:
        #     collection.delete_one(filtro)
        # adicionar ou reacidionar o registro 
        insert_result = collection.insert_one(fii)
        # print("Inserted ID:", insert_result)
        # fii.pop("_id", None)
        fii["_id"] = str(fii["_id"])
    client.close()
    # print(json_list)


def insertMysqlDb(json_list):
    print('====== GUARDAR DADOS NO BANCO DE DADOS MYSQL==========================')

    # guardar os dados no banco de dados local
    cnx = conectar_banco()
    cursor = cnx.cursor()
    query = '''INSERT INTO fii (dy, ticker, liquidez_dia, pvp, patrimonio_liq, 
                preco_atual, qtd_ativos, rent_acum, setor, ult_dividendo) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    
    fiis = json_list
    dados = ()
    for fii in fiis:
        dy = float(fii['DIVIDEND YIELD'].replace(" %", "").replace(",", "."))
        ticker = fii['FUNDOS']
        liquidez_d = float(fii['LIQUIDEZ DIÁRIA (R$)'].replace(".", "").replace(",","."))
        pvp = float(fii['P/VP'].replace(",","."))
        patrimonio_liq = float(fii['PATRIMÔNIO LÍQUIDO'].replace(".", "").replace(",","."))
        preco_atual = float(fii['PREÇO ATUAL (R$)'].replace(".", "").replace(",","."))
        qdt_ativos = int(fii['QUANT. ATIVOS'])
        rent_acum = float(fii['RENTAB. ACUMULADA'].replace(" %", "").replace(",", "."))
        setor = fii['SETOR']
        ult_dividendo = float(fii['ÚLTIMO DIVIDENDO'].replace(",", "."))

        dados += (dy, ticker, liquidez_d, pvp, patrimonio_liq, preco_atual, qdt_ativos, rent_acum, setor, ult_dividendo)
        print("xxxxxxx", dados)
    print('*******************************************', query)
    cursor.execute(query, dados)
    cnx.commit()
    cursor.close()
    cnx.close()

@app.route('/fii_data', methods=['GET'])
def fii_data():
    print('Endpoint de carga dos dados no banco ...')
    fii_tickers = request.args.get('tickers')
    # print(fii_tickers)
    tickers = []
    if fii_tickers != None:
        tickers = fii_tickers.split(',')
        # print('tickers: ', tickers)
        tickers = [x.upper() for x in tickers]
        # print('tickers: ', len(tickers))
    else:
        tickers = []

    driver_path = 'C:\\Users\\Isaac\Documents\\python_workspace\\api_fiis\\chromedriver.exe'
    options = ChromeOptions()
    options.add_argument("--headless")
    driver = Chrome(executable_path=driver_path, options=options)
    try :
        driver.set_window_size(1280, 900)
        driver.get(f'https://www.fundsexplorer.com.br/ranking')

        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CLASS_NAME, "default-fiis-table__container__table")))
        # print('########################### ')
        #tbl = driver.find_element_by('table-ranking')
        tbl = driver.find_element(By.CLASS_NAME, 'default-fiis-table__container__table')
        # print(tbl)
        headers = tbl.find_elements_by_xpath("//table[@class='default-fiis-table__container__table']/thead/tr[1]/th")
        trs = tbl.find_elements_by_xpath('//tbody/tr')

        cols = []
        for header in headers:
            # print(header.text)
            cols.append(header.text.replace('\n', ' '))
        
        # print('col size: ', len(cols))

        json_list = []
        qtd = 1000000
        index_qtd = 0
        for tr in trs:
            if index_qtd == qtd:
                break

            tds = tr.find_elements_by_xpath("td")
            index = 0
            json = {}
            # print('tds size: ', len(tds))
            fl_present = True
            for td in tds:
                # print(td.text.strip().upper(), ' ==> ', (td.text.strip().upper() not in tickers))
                if index == 0 and len(tickers) > 0 and len(tickers) > 0 and td.text.strip() not in tickers :
                    fl_present = False
                    # print(td.text)
                    break
                # print('index: ', index)
                json[sanitizarKey(cols[index])] = td.text
                index+=1
                
            # print(json)
            index_qtd+=1
            if fl_present == True:
                json["TIMESTAMP"] = datetime.now()
                json_list.append(json)

        insertMongoDB(json_list)        
    except Exception as e:
        print('erro ao buscar o dado na página ', e)
    finally:
        driver.quit()
        return json_list

# Definir um codificador personalizado para o objeto ObjectId
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return str(o)
        return super().default(o)

@app.route('/fii_data_xml', methods=['GET'])
def obterDadosTickerXML():
    ticker = request.args.get('ticker')
    # print(ticker)
    client = MongoClient("mongodb://192.168.0.190:27017")
    db = client.fii
    collection = db.fii
    filtro = {"FUNDOS" : ticker.upper()}
    resultado = collection.find(filtro).sort("TIMESTAMP", -1)
    json_list = json.loads(json.dumps(list(resultado), cls=JSONEncoder))
    client.close()
    # print(json_list)
    # print("============--------------------")
    root = ET.Element("fiis")

    for item in json_list:
        fii = ET.SubElement(root, "fii")
        for key, value in item.items():
            campo = ET.SubElement(fii, sanitizarKey(key))
            campo.text = str(value)

    tree = ET.ElementTree(root)
    xml_string = ET.tostring(root, encoding="utf-8", method="xml")

    # xml = xmltodict.unparse(json.dumps(list(resultado), cls=JSONEncoder), pretty=True)
    # print(xml_string)
    return Response(xml_string, content_type='application/xml')


def sanitizarKey(key):
    return unidecode(key.replace("/","").replace(" ", "_").replace("(", "").replace(")", "").replace(".","").replace("$",""))

@app.route('/melhores_fiis', methods=['GET'])
def obterMelhoresAtivosCompra():
    # obter a relacao de todos os ativos para selecionar os melhores com base nos critérios desejados
    cliente = MongoClient("mongodb://192.168.0.190:27017")
    banco_de_dados = cliente["fiis"]
    colecao_fiis = banco_de_dados["fiis"]
    data_maxima = colecao_fiis.find_one(sort=[("TIMESTAMP", pymongo.DESCENDING)])
    #  Desconsiderar a hora na data máxima
    data_maxima = data_maxima["TIMESTAMP"].replace(hour=0, minute=0, second=0, microsecond=0)

    # filtrar fundos com quantidade de ativos maior ou igual a 5
    pipeline = [
                    {
                        "$addFields": {
                            "quant_ativos_int": {"$toInt": "$QUANT_ATIVOS"},
                        }
                    },
                    {
                        "$addFields": { # remove os N/A da liquidez diaria para nao dar pau na hora de converter pra float
                            "liquidez_diaria_sem_na": {
                                "$replaceAll": {
                                    "input": "$LIQUIDEZ_DIARIA_R",        
                                    "find": "N/A",   
                                    "replacement": "0" 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # remove os pontos da liquidez diaria 
                            "liquidez_diaria_sem_pontos" : {
                                "$replaceAll": {
                                    "input": "$liquidez_diaria_sem_na",        
                                    "find": ".",   
                                    "replacement": "" 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # troca a virgula por ponto na liquidez diaria 
                            "liquidez_diaria_sem_pontos_sem_virgula" : {
                                "$replaceAll": {
                                    "input": "$liquidez_diaria_sem_pontos",        
                                    "find": ",",   
                                    "replacement": "." 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # converte a liquidez diaria para double pra filtrar 
                            "liquidez_diaria_float" : {
                                "$toDouble": "$liquidez_diaria_sem_pontos_sem_virgula"
                            }
                        }
                    },
                    {
                        "$addFields": { # remove os N/A da patrimonio_liquido para nao dar pau na hora de converter pra float
                            "patrimonio_liquido_sem_na": {
                                "$replaceAll": {
                                    "input": "$PATRIMONIO_LIQUIDO",        
                                    "find": "N/A",   
                                    "replacement": "0" 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # remove os pontos da patrimonio_liquido
                            "patrimonio_liquido_sem_pontos" : {
                                "$replaceAll": {
                                    "input": "$patrimonio_liquido_sem_na",        
                                    "find": ".",   
                                    "replacement": "" 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # troca a virgula por ponto na patrimonio_liquido
                            "patrimonio_liquido_sem_pontos_sem_virgula" : {
                                "$replaceAll": {
                                    "input": "$patrimonio_liquido_sem_pontos",        
                                    "find": ",",   
                                    "replacement": "." 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # converte a patrimonio_liquido para double pra filtrar 
                            "patrimonio_liquido_float" : {
                                "$toDouble": "$patrimonio_liquido_sem_pontos_sem_virgula"
                            }
                        }
                    },
                    {
                        "$addFields": { # troca a virgula por ponto na liquidez diaria 
                            "pvp_ponto" : {
                                "$replaceAll": {
                                    "input": "$PVP",        
                                    "find": ",",   
                                    "replacement": "." 
                                }
                            }
                        }
                    },
                    {
                        "$addFields": { # converte a patrimonio_liquido para double pra filtrar 
                            "pvp_float" : {
                                "$toDouble": "$pvp_ponto"
                            }
                        }
                    },
                    {   # Dados mais recentes
                        "$match": {
                            "TIMESTAMP": {"$gte": data_maxima}
                        }
                    },
                    {   # liquidez diaria > 500k
                        "$match": {
                            "liquidez_diaria_float": {"$gte": 500000}
                        }
                    },
                    {   # patrimonio_liquido > 500M
                        "$match": {
                            "patrimonio_liquido_float": {"$gte": 500000000}
                        }
                    },
                    {   # mais que 5 imoveis ou 0 que é fundo de papel 
                        "$match": {
                            "$or": [
                                {"quant_ativos_int": {"$gte": 5}},
                                {"quant_ativos_int": 0}
                            ]
                        }
                    },
                    {   # remove os campos temporarios usado para conversao e filtro 
                        "$project": {
                            "quant_ativos_int": 0,  # Opcional: para excluir o campo temporário
                            "liquidez_diaria_sem_pontos": 0,
                            "liquidez_diaria_sem_pontos_sem_virgula": 0,
                            "liquidez_diaria_float": 0,
                            "liquidez_diaria_sem_na": 0,
                            "patrimonio_liquido_sem_pontos": 0,
                            "patrimonio_liquido_sem_pontos_sem_virgula": 0,
                            "patrimonio_liquido_float": 0,
                            "patrimonio_liquido_sem_na": 0,
                            "DY_12M_ACUMULADO": 0,
                            "DY_12M_MEDIA": 0,
                            "DY_3M_ACUMULADO": 0,
                            "DY_3M_MEDIA": 0,
                            "DY_6M_ACUMULADO": 0,
                            "DY_6M_MEDIA": 0,
                            "PVPA": 0,
                            "DY_ANO": 0,
                            "DY_PATRIMONIAL": 0,
                            "TIMESTAMP": 0,
                            "Vacancia Financeira": 0,
                            "_id": 0,
                            "RENTAB_ACUMULADA": 0,
                            "RENTAB_PATR_ACUMULADA": 0,
                            "RENTAB_PATR_PERIODO": 0,
                            "RENTAB_PERIODO": 0,
                            "VACANCIA_FINANCEIRA": 0,
                            "VARIACAO_PATRIMONIAL": 0,
                            "VARIACAO_PRECO": 0,
                        }
                    },
                    {
                        "$sort": {
                            "PVP": 1,
                            "PATRIMONIO_LIQUIDO": 1,
                            "LIQUIDEZ_DIARIA_R": 1
                        }
                    }
                ]

    fiis_fitro_inicial = colecao_fiis.aggregate(pipeline)
    json_list_fiis_filtro_inicial = json.loads(json.dumps(list(fiis_fitro_inicial), cls=JSONEncoder))

    # depois de selecionar os fiis que se encaixam nos criterios iniciais de compra 
    # vamos validar os ativos que ja tenho em carteira
    colecao_carteira = banco_de_dados["wallet"]
    carteira = colecao_carteira.find_one({})
    # print('XXXXXXXXXXXXXXXXXXXXXXX fundos ', carteira["FUNDOS"])

    # verificar em cada ativo da carteira do que tem menor posicao para o que tem maior posicao
    # quais estao na lista dos filtros iniciais para indicar compra 
    i = 1
    fiis_bom_pra_compra = []
    for fii_carteira in carteira["FUNDOS"]:
        print("=============================================================================")
        print(str(i), ' ',  fii_carteira["FUNDO"])
        print("=============================================================================")
        i+=1

        # verificar se o ativo ta presente na lista filtrada 
        for fiis_filtro in json_list_fiis_filtro_inicial:
            # print(f'*************** {fii_carteira["FUNDO"]} == {fiis_filtro["FUNDOS"]}')
            if fii_carteira["FUNDO"] == fiis_filtro["FUNDOS"] and fiis_filtro["pvp_float"] <= 1:
                print('!!!!!! BOM PRA COMPRA')
                fiis_bom_pra_compra.append(fiis_filtro)
                
    json_list_fiis_bom_pra_compra = json.loads(json.dumps(list(fiis_bom_pra_compra), cls=JSONEncoder))

    json_final = {}
    json_final["ativos_filtro"] = json_list_fiis_filtro_inicial
    json_final["ativos_carteira"] = json_list_fiis_bom_pra_compra

    return json_final




app.run()