import pandas as pd
import numpy as np
import hashlib

def hash_string(string):   
    ''' Genera un hash (identificador) único a partir de una cadena de texto.
        Nota: el resultado es totalmente replicable, siempre va a generar el mismo resultado'''
    string = string.encode('utf-8')
    return hashlib.sha512(string).hexdigest()  

def genera_id(df, id_columns):
    ''' Genera id a partir de la lista de columnas del df 

        params:
        - df (pd.DataFrame)
        - id_columns (lista): lista que contenga los nombres de las columnas utilizadas para generar el id
        - id_name (string): nombre de la columna generada con el id
        
        returns:
        - id_array (pd.Series): columna con los ids generado, en el orden ingresado.
        
        TODO: agregar que modifique las columnas necesarias para garantizar que non haya duplicados... Quizas construir una función aparte
    '''
    df = df.copy()
    
    if 'ronda' in id_columns:
        id_columns = [col for col in id_columns if 'ronda' not in col] + ['ronda']
    
    df[id_columns] = df[id_columns].fillna(0)
    df['hash_content'] = df[id_columns].astype(str).apply(lambda x: x.str[:40]).sum(axis=1) # Nota: solo uso los primeros 40 characters de cada string
    df['id'] = df['hash_content'].apply(hash_string)
    
    hash_null = hash_string('0'*len(id_columns))
    hash_null_r1 = hash_string('0'*(len(id_columns)-1) + '1')    
    hash_null_r2 = hash_string('0'*(len(id_columns)-1) + '2')    
    
    df['id'] = np.where(
                    (df['id'] == hash_null) | (df['id'] == hash_null_r1) | (df['id'] == hash_null_r2), 
                    np.nan,
                    df['id']
               )
    
    return df['id']

def isNaN(value):
    ''' Fuente: https://www.codespeedy.com/check-if-a-given-string-is-nan-in-python/ '''
    # Aprovecha la propiedad de que los nan no son iguales a si mismos
    return value != value

def compara_columnas(df, indexes):
    ''' Muestra las columnas diferentes entre dos observaciones de un mismo dataframe
    
        params:
        - df (pd.DataFrame): debe contener ambas observaciones
        - indexes (list of int): lista de indices del df que contengan las observaciones que se quieren comparar. Deben ser únicos en el df.    
    '''
    from IPython.display import display
    
    lista_var_diff = []
    for var in df.columns:
        lista_valores = [df.loc[index,var] for index in indexes]
        lista_valores_unicos = set(lista_valores)
        
        # Si el set es menor o igual a uno es que todos los valores son iguales!
        if len(lista_valores_unicos) <= 1:
            pass

        # Si hay diferencias
        else:
            # Si son diferentes pero todos son nan, no cuenta como diferencia:
            if all([isNaN(df.loc[index,var]) for index in indexes]):
                pass
            else:
                lista_var_diff += [var]
                
    if len(lista_var_diff)>0:
        print("Son diferentes en las siguientes columnas:")
        display(df.loc[indexes,lista_var_diff])
        son_duplicados = False
     
    else:
        print("Son iguales...")
        son_duplicados = True
    
    return son_duplicados

def compare_strings_elementwise(cases):
    ''' Compara dos strings caracter por caracter, reflejando las diferencias entre ambos. '''
    
    import difflib

    for a,b in cases:     
        for i,s in enumerate(difflib.ndiff(a, b)):
            if s[0]==' ': continue
            elif s[0]=='-':
                print(u'Delete "{}" from position {}'.format(s[-1],i))
            elif s[0]=='+':
                print(u'Add "{}" to position {}'.format(s[-1],i))    
        print()      
        
    return

def limpia_columnas_duplicadas(df, index1, index2, force=True):
    ''' Elimina columnas duplicadas no detectadas automáticamente, indicando los indices de esas columnas.
    
        params:
        - df (pd.DataFrame): debe contener ambas observaciones
        - index1, index2 (int): indices del df que contengan las observaciones que se quieren comparar. Deben ser únicos.    
        
        return:
        - df (pd.DataFrame): df dropeando index2.
    '''
    from IPython.display import display
    
    if force==False:
        son_duplicados = compara_columnas(df, index1, index2)
        if son_duplicados==True:
            pass
        else:
            # Si no forceo y no son iguales, no limpia las columnas
            return
        
    df = df.drop(index2, axis=0)
    return df

def muestra_diferencias_entre_id_duplicados(df, id_name, 
                                            edit_id=False, edit_id_name=None, cols_genera_id=None,
                                            df_master=None):
    ''' Presenta las columnas diferentes para cada valor de id_name duplicado. Si edit_id==True, edita el nombre del edit_id_name para que 
    sea diferente y, al volver a generar
    
    '''
    if type(df_master)!=type(pd.DataFrame()):
        print("Se usará el df como master.")
        df_master = df
        
    # Identifico las respuestas con id duplicado
    duplicates_respuesta = df[(df[id_name].duplicated(keep=False)) & (df[id_name].notnull())]
    # Construyo grupos según mismo id
    duplicates_respuesta['grupo'] = duplicates_respuesta.groupby(by=id_name).ngroup()
    lista_grupos = duplicates_respuesta.grupo.sort_values().unique()
    print("Hay", len(lista_grupos), "grupos de ids duplicados. Chequeando diferencias dentro de cada grupo...\n")

    for g in lista_grupos:
        print("#################   Grupo", g, "  ##################")
        indexes = duplicates_respuesta[duplicates_respuesta.grupo == g].index
        compara_columnas(df, indexes)
        print("#################################################\n")
        
        if edit_id:
            df_master.loc[indexes[0],edit_id_name] = str(df_master.loc[indexes[0],edit_id_name]) + '1' 
            df_master.loc[indexes[1],edit_id_name] = str(df_master.loc[indexes[1],edit_id_name]) + '2'     
    
    if edit_id:
        print("Modificando id...")
        df_master[id_name] = genera_id(df_master,cols_genera_id)
        return df_master
    else:
        return
        
def normaliza_columnas_post_merge(data, suffixes=('','_y')):
    ''' Elimina columnas duplicadas y corrige sufijos luego de un merge. 
    
        Luego de un df.merge, combina las columnas de la base left y right (master y usign en Stata) llenando los np.nan de cada columna
        con los valores de la otra base. Finalmente, crea una nueva columna sin los sufijos y elimina las dos columnas duplicadas. Ante 
        discrepancias entre ambas columnas, se queda con la de la base left (master) y muestra en la consola las diferencias.

        params:
        df (pd.DataFrame): DataFrame despues de realizar un merge
        suffixes (tuple of size 2): sufijos de las columnas duplicadas en el merge. Debe ser igual al parámetro suffixes de pd.DataFrame.merge()) 
    
        returns:
        df (pd.DataFrame): DataFrame con la combinación
    '''
    
    from IPython.display import display
    from pandas.api.types import is_numeric_dtype

    # Obtengo lista con las columnas con el sufijo de la derecha
    lista_duplicados = [x for x in data.columns if suffixes[1] in x] 
    # Obtengo el largo del sufijo de la izq
    der_suf_len = len(suffixes[1])
    
    # Loopeo por variables
    for var in lista_duplicados:

        var_name = var[:-der_suf_len]
        
        # Obtengo el nombre de la variable sin el sufijo
        var_izq = var_name + suffixes[0] # Quito del nombre de la columna el sufijo derecho y agrego el izquierdo
        var_der = var_name + suffixes[1]
        
        # Completo con los valores de la otra columna 
        data[var_izq] = np.where((data[var_izq].isna()),data[var_der],data[var_izq])
        data[var_der] = np.where((data[var_der].isna()),data[var_izq],data[var_der])
        
        # Redondeo los números para garantizar que sean comnparables
        if is_numeric_dtype(data[var_izq]):
            data[var_izq] = data[var_izq].round(4)
            data[var_der] = pd.to_numeric(data[var_der], errors='coerce').round(4)
            
        if is_numeric_dtype(data[var_der]):
            data[var_der] = data[var_der].round(4)
            data[var_izq] = pd.to_numeric(data[var_izq], errors='coerce').round(4)
            
        # Corroboro si no hay valores diferentes entre ambas columnas:
        if any(data[var_der].fillna('0')!=data[var_izq].fillna('0')):
            print("Hay valores diferentes en las columans",var_der, "y", var_izq)
            display(data[data[var_der].fillna('0')!=data[var_izq].fillna('0')][[var_izq,var_der]])
        
        data[var_name] = data[var_izq]
        data = data.drop(columns=[var_izq,var_der])
        
    return data

def crea_lista_de_archivos(path_datain, extension):
    ''' Creo lista con los archivos que se encuentren en la carpeta path_datain y que además terminen con la
    extension indicada. La lista está ordenada alfabéticamente, pero los números están por orden numérico 
    (i.e. 10 NO está justo despues de 1, sino que van de 1,2,3,...,9,10,11,...'''

    import re
    import glob

    # Creo lista de archivos
    files = []
    for infile in sorted(glob.glob(path_datain + "\*." + extension)):
        files += [infile]

    # Ordeno la lista
    files = sorted(files)
    # Elimino elementos temporales que empiezan con "~$"
    files = [ x for x in files if "~$" not in x ]
    return files

def importa_y_genera_ids(path, extension, id_name, id_columns):
    ''' Importa todos los archivos dentro de una carpeta con el formato solicitado y genera el id correspondiente.
    
        params:
        path (str): Ruta de acceso a la carpeta
        extension (str): formato de los archivos (ej: '.xlsx')
        id_name (str): nombre de la columna de id que se generará
        id_columns (list of str): lista con el nombre de las columnas que identifican el id

        return:
        data (pd.DataFrame): dataframe con los input appendeados y la columna id generada.
    '''
    
    files = crea_lista_de_archivos(path, extension)
    print("Hay",len(files),"archivos con la extension establecida dentro de la carpeta.")

    data = []
    for file in files:
        data_temp = pd.read_excel(file)
        data += [data_temp]
    data = pd.concat(data)

    data = data.reset_index(drop=True)
    data[id_name] = genera_id(data, id_columns)
    print("La base tiene", len(data), "observaciones.")

    return data

def limpia_variable_dates_old(df, column_dates):
    ''' Transforma las fechas en formato de descarga de los mails a datetime.
    
        params:
        df (pd.DataFrame): DataFrame con dates en el formato crudo de mail
        column_dates (array of strings): lista o tupla con los nombres de las columnas a limpiar
        
        return:
        df_dates (pd.DataFrame): DataFrame con las columnas limpias. Tiene dimension igual al largo de column_dates. 
    '''
    
    df_dates = []
     
    for date_name in column_dates:
        
        df[date_name + '_clean'] = df[date_name].str.replace(".+,\s","",regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace("\-|\+|(0\d00)(.*)","",regex=True) #FIXME: REVISAR QUÉ ES -0500 y si está en Arg o Esp
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace("GMT","")
        df[date_name + '_clean'] = df[date_name + '_clean'].str.lstrip(' ').str.rstrip(' ')
        df[date_name + '_clean'] = df[date_name + '_clean'].str.zfill(20)
        df[date_name + '_clean'] = pd.to_datetime(df[date_name + '_clean'], format="%d %b %Y %H:%M:%S")
        df_dates += [df[date_name + '_clean']]
        
    df_dates = pd.concat(df_dates, axis=1)
    return df_dates


def limpia_variable_dates(df, column_dates):
    ''' Transforma las fechas en formato de descarga de los mails a datetime.
    
        params:
        df (pd.DataFrame): DataFrame con dates en el formato crudo de mail
        column_dates (array of strings): lista o tupla con los nombres de las columnas a limpiar
        
        return:
        df_dates (pd.DataFrame): DataFrame con las columnas limpias. Tiene dimension igual al largo de column_dates. 
    '''
    
    df_dates = []
     
    for date_name in column_dates:
        
        df[date_name + '_clean'] = df[date_name].str.replace(".+,\s","",regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('UTC','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('GMT','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('EDT','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('PDT','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('ART','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('\(','', regex=True).str.replace('\)','', regex=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].str.replace('\(-03\)','', regex=True)
        
        df[date_name + '_clean'] = df[date_name + '_clean'].str.lstrip(' ').str.rstrip(' ')
        df[date_name + '_clean'] = df[date_name + '_clean'].str.zfill(20)
        
        df[date_name + '_clean'] = pd.to_datetime(df[date_name + '_clean'], 
                                                    format="%d %b %Y %H:%M:%S %z", 
                                                    utc=True, infer_datetime_format=True)
        df[date_name + '_clean'] = df[date_name + '_clean'].dt.tz_convert('America/Argentina/Buenos_Aires')
        df[date_name + '_clean'] = df[date_name + '_clean'].dt.tz_localize(None)
        df_dates += [df[date_name + '_clean']]
        
    df_dates = pd.concat(df_dates, axis=1)
    return df_dates


def genera_orden_de_respuestas(df, group_name, sort_name):
    ''' Genera variable con el orden de llegada de las observaciones (respuestas). 
    
        Las respuestas son agrupadas por la variable group_name
        (por ejemplo, from_inmob, es decir, el correo de la cada inmobliliaria) y ordenadas según la variable sort_name (por ejemplo, la hora
        de recepción del mensaje),

        params:
        df (pd.DataFrame)
        group_name (string)
        sort_name (string or number)
    '''
    
    df_temp = df    
    df_temp.sort_values(by=sort_name, inplace=True)
    df_temp['uno'] = 1
    df_no_nulos = df_temp[df_temp[group_name].notnull()]
    df_temp['order'] = df_no_nulos.groupby(by=group_name)['uno'].cumsum()

    return df_temp['order']

def deshace_matches(df, inmob, ronda):

    import numpy as np
    import pandas as pd
    
    # Construyo df con las observaciones que quiero agregar: 
    #   to_append_pub: la base que solo contiene info de la publicacion
    to_append_pub = df.loc[(df['ronda']==ronda) & (df['inmobiliaria']==inmob) & (df['tipo']=='match_ambas'),:'inmobiliaria_c']
    to_append_pub = to_append_pub[to_append_pub.duplicated()==False]
    to_append_pub['tipo'] = 'solo_publicacion'
    print('Cantidad de publicaciones unicas:', len(to_append_pub))
    #   to_append_rta: la base que solo contiene info de la respuesta
    to_append_rta = df.loc[(df['ronda']==ronda) & (df['inmobiliaria']==inmob) & (df['tipo']=='match_ambas'),'subject_inmob':]
    to_append_rta = to_append_rta[to_append_rta.duplicated()==False]
    to_append_rta['tipo'] = 'solo_respuesta'
    to_append_rta['ronda'] = ronda
    to_append_rta[['match_confiab_inmob','match_step_inmob','match_string_inmob','match2_string_inmob']] = np.nan

    print('Cantidad de respuestas unicas:', len(to_append_rta))
    print('ID Respuestas:', list(set(to_append_rta.id_respuesta.to_list())))
    #Indentifico las observaciones que quiero dropear
    df_to_drop = df[(df['ronda']==ronda) & (df['inmobiliaria']==inmob) & (df['tipo']=='match_ambas')]
    pubs_to_drop = list(set(df_to_drop.id_publicacion.to_list()))

    print("Voy a deshacer",len(df_to_drop),"matches.\n")
    print('ID publicación:',pubs_to_drop)
    print("Inmobiliaria:",inmob)
    print("Ronda",ronda)

    print("Anteriormente, en el df habia",len(df),"filas.\n")
    df = df[-df.id_publicacion.isin(pubs_to_drop)]

    # Agrego las filas dropeadas, separadas entre respuesta y publicacion (ie "deshago" el match)
    df = pd.concat([df,to_append_pub,to_append_rta])
    print("Ahora hay",len(df),"filas.\n")
    print("#########################################")
    return df

def chequeo_contaminacion_cruzada(df_errores, lista_grupo, lista_tratamiento):
    ''' Verifica si hay contaminación cruzada entre tratamientos para observaciones similares.
        
        Verifica si, para grupos definidos como observaciones con todos los elementos de lista_grupo iguales, las variables de lista_tratamiento son diferentes.
    
        params:
        df (pd.DataFrame)
        lista_grupo (list): lista con las columnas que identifican el grupo
        lista_tratamiento (list): lista con las columnas que identifican el tratamiento.
    '''

    df_errores['grupo'] = df_errores.groupby(by=lista_grupo).ngroup()
    grupos = df_errores['grupo'].unique()
    print("Hay", len(grupos),"potenciales inmobiliarias mal enviadas en la base.")

    contaminados = []

    for g in grupos:
        
        tratamientos_en_el_grupo = len(df_errores[df_errores['grupo']==1][lista_tratamiento].drop_duplicates())
        if tratamientos_en_el_grupo==1:
            pass
        else:
            print("El grupo", g, "tiene contaminación cruzada.")
            contaminados += [g]

    print("Hay", len(contaminados), "gurpos contaminados")
    
    return

def limpia_prueba_google_y_quien_sos(df, path_dropbox, from_name='from', subject_name='subject'):
    import sys
    sys.path.append(path_dropbox + r'\BID-LGBTQ+\campo\code\mails\matches')
    import matching_v1 as matching
    #### Elimino observaciones que se corresponden a mails de prueba o de Google

    # Dropeo observaciones que tienen Google en el mail / user
    prev = len(df)
    df_out = df[df[from_name].str.contains("Google", na=False) == True]
    df =     df[df[from_name].str.contains("Google", na=False) != True]
    print("Se dropearon", prev - len(df), "observaciones de Google.")

    # Dropeo observaciones de prueba de Juli
    prev = len(df) 
    to_concat = df[df[from_name].str.contains("Julian test", na=False) == True]
    df = df[df[from_name].str.contains("Julian test", na=False) != True]
    df_out = pd.concat([df_out,df[df[from_name].str.contains("Julian test", na=False) == True]])

    print("Se dropearon", prev - len(df), "observaciones ('Julian Test').")

    # Dropeo observaciones de prueba
    prev = len(df) 
    to_concat = df[df[subject_name].str.contains("prueba", na=False) == True]
    df = df[df[subject_name].str.contains("prueba", na=False) != True]
    df_out = pd.concat([df_out,to_concat])
    print("Se dropearon", prev - len(df), "observaciones 'de prueba'.")

    # Base de correos desde los que enviamos
    mails_df = pd.read_excel(path_dropbox + r"\BID-LGBTQ+\campo\0_instrucciones\2-Emails para Intervención.xlsx")
    mails_df['Usuario_clean'] = mails_df.Usuario.apply(matching.clean_mail)
    users = mails_df['Usuario_clean'].to_list()

    # Dropeo observaciones de prueba (from == to, ¡nos los autoenviamos!)
    prev = len(df) 
    for user in users:
        to_concat = df[df[from_name].str.contains(user)==True]
        df = df[df[from_name].str.contains(user)!=True]
    df_out = pd.concat([df_out,to_concat])
    print("Se dropearon", prev - len(df), "observaciones de mails nuestros.")
        
    # Dropeo Mail Delivery Subsystem
    prev = len(df) 
    to_concat = df[df[from_name].str.contains("Mail Delivery Subsystem", na=False) == True]
    df = df[df[from_name].str.contains("Mail Delivery Subsystem", na=False) != True]
    if 'id_publicacion' in df.columns:
        df = df[-df['id_publicacion'].astype(str).str.contains("MDS", na=False)]
    df_out = pd.concat([df_out,to_concat])
    print("Se dropearon", prev - len(df), "observaciones de MDS.")

    # Dropeo observaciones de ¿Quien sos?
    pre = len(df)
    df[subject_name].fillna('', inplace=True)
    df = df[df[subject_name].str.contains('Consulta propiedad Properati')!= True]
    print("Se dropearon", pre - len(df), "de ¿Quien sos?")

    return df

def incorpora_matches_manuales(df_main, rtas_ra, pubs_ra, ronda, vars_pubs, vars_rtas):
    ''' 
    
    '''
    
    from IPython.display import display
    # Función de reconstrucción de base

    # 1) Chequeo que "solo_respuesta"   del main coincida con "respuestas"    enviados a RA, y
    #            que "solo_publicacion" del main coincida con "publicaciones" enviados a RA 
    df_main_r1 = df_main[df_main['ronda']==ronda]
    df_main_rtas = df_main_r1[(df_main_r1['tipo']=='match_ambas') | (df_main_r1['tipo']=='solo_respuesta')  ]
    df_main_pubs = df_main_r1[(df_main_r1['tipo']=='match_ambas') | (df_main_r1['tipo']=='solo_publicacion')]

    test_rtas = df_main_rtas.merge(rtas_ra, on='id_respuesta'  , indicator=True, how='outer')
    test_rtas = test_rtas[test_rtas._merge == 'right_only'].dropna(axis=1, how='all')
    if len(test_rtas)>0:
        print("Hay respuestas que fueron categorizadas por RA y no están en la base main. Son las siguientes:")    
        display(test_rtas)
    test_pubs = df_main_pubs.merge(pubs_ra, on='id_publicacion', indicator=True, how='outer')
    test_pubs = test_pubs[test_pubs._merge == 'right_only'].dropna(axis=1, how='all')
    if len(test_pubs)>0:
        print("Hay publicaciones que fueron enviados a los RA y no están en la base main. Son las siguientes:")    
        display(test_pubs)
    
    ### 2) Spliteo la base en publicacion y respuestas (sin matcheos)
    df_main_pubs = df_main[vars_pubs]
    df_main_rtas = df_main[vars_rtas]

    ### 3) Incorporo las matcheadas manualmente 
    new_matched = df_main_pubs.merge(rtas_ra , on='id_publicacion'  , indicator=True, how='outer')
    new_matched = new_matched[new_matched['_merge']=='both']
    new_matched = new_matched.drop(columns='_merge')
    new_matched = normaliza_columnas_post_merge(new_matched, suffixes=('_x','_y'))

    lista_new_matched_rta = new_matched.id_respuesta.to_list()
    lista_new_matched_pub = new_matched.id_publicacion.to_list()
    
    ### 4) Reconstruyo la base:
    # Elimino los que acabo de matchear
    print(sum(((df_main['id_respuesta'].isin(lista_new_matched_rta)) & (df_main['tipo']=='solo_respuesta'))))
    df_main = df_main[-((df_main['id_respuesta'].isin(lista_new_matched_rta)) & (df_main['tipo']=='solo_respuesta'))]
    print(sum((df_main['id_publicacion'].isin(lista_new_matched_pub)) & (df_main['tipo']=='solo_publicacion')))
    df_main = df_main[-((df_main['id_publicacion'].isin(lista_new_matched_pub)) & (df_main['tipo']=='solo_publicacion'))]

    # Agrego los que acabo de matchear
    df_main = pd.concat([df_main, new_matched])
    
    return df_main, test_rtas, test_pubs, new_matched