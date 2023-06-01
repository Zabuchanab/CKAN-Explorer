import pandas as pd
from typing import Dict, Any
import requests
import plotly_express as px

class Ckan: 
    
    def __init__(self,base_url_ckan:str):
        self.url = base_url_ckan

    def __repr__(self) -> str:
        return (f"Funciones para interactuar con API-CKAN de {self.url}")
    
    @property
    def base_url(self) -> str:
        """
        Devuelve URL base de ckan enviada
        """
        return f'{self.url}/api/action'

    @property
    def endpoints(self) -> Dict[str,str]:
        """
        Listado de enpoints donde que se consultan a la API
        de CKAN provista por la url base con la cual se trabaja
        """
        endpoints = {
            'datasets':'package_list',
            'grupos':'group_list',
            'tags':'tag_list',
            'organizaciones':'organization_list'
        }
        return endpoints
    
    def extractor(self,endpoint:str) -> Dict[str,Any]:
        """
        Función base extractor que se conecta a la fuente y extrae en función
        del endpoint que se pase como parámetro. El endpoint que se pase tiene 
        que ser un clave-valor guardado en self.endpoints como tal.
        """
        try:
            url = f'{self.base_url}/{endpoint}'
            respuesta = requests.request(method='GET',
                                         url=url)
            return respuesta.json()['result']
        except BaseException as error:
            return error
        
    def listado_datasets(self) -> list:
        """
        Retorna el listado de datasets publicados en el CKAN.
        """
        return self.extractor(endpoint=self.endpoints['datasets'])
    
    def listado_grupos(self) -> list:
        """
        Retorna el listado de grupos publicados en el CKAN.
        """
        return self.extractor(endpoint=self.endpoints['grupos'])
    
    def listado_etiquetas(self) -> list:
        """
        Retorna el listado de etiquetas de los recursos.
        """
        return self.extractor(endpoint=self.endpoints['tags'])
    
    def listado_organizaciones(self) -> list:
        """
        Retorna el listado de organizaciones
        """
        return self.extractor(endpoint=self.endpoints['organizaciones'])
    
    def df_recursos_totales(self) -> pd.DataFrame:
        """
        Retorna el listado de recursos disponibles en el CKAN
        con la siguiente información:
        1. Conjunto de datos al que pertenece.
        2. Descripción del recurso.
        3. Fecha de creación y modificación.
        4. Formato del recurso.
        """
        recurso_total = []

        for d in self.listado_datasets():
            e = f'package_show?id={d}'
            r = self.extractor(endpoint=e)['resources']

            campos = {'name',
                      'created',
                      'description',
                      'format',
                      'last_modified',
                      'metadata_modified'}
            
            resultado = [{k:v for k, v in x.items() \
                          if k in campos} for x in r]
            
            for i in resultado:
                i['dataset'] = d
                recurso_total.append(i)

        df = pd.DataFrame(recurso_total)
        df['created'] = pd.to_datetime(df['created'])\
            .apply(lambda x: x.strftime(f'%m/%d/%Y'))
        
        df.sort_values(by='created',
                       inplace=True,
                       ascending=False)
        
        return df
    
    def viz_actividad_apertura(self) -> px.line:
        """
        Visualización temporal de publicación de recursos
        en el CKAN instanciado.
        """
        df = pd.DataFrame(
            self.df_recursos_totales()\
            .groupby(by='created')\
                .size()).reset_index()
        
        df.columns = ['fecha','recursos_subidos']

        df['fecha'] = pd.to_datetime(df['fecha'])

        df.sort_values(by='fecha',
                       inplace=True,
                       ascending=True)

        figura = px.line(df,
                         x='fecha',
                         y='recursos_subidos',
                         line_shape='spline',
                         markers=True)
        
        return figura