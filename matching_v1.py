###############################################
############## Paquete Matching. ##############
###############################################

####### Autor: Nico Abbate
####### Version: 1.0.1
####### Fecha: 13-4-2022

# !py -m pip instaall Levenshtein 
# !py -m pip install fuzzysearch 

##### Agregar:
##### Solo buscar por mail incluyendo puntos!!! 

### Limpieza
def clean(string, space=False):
    '''Limpia un string. Pasa a minúsculas, elimina espacios (opcional) y caractéres no alfanuméricos.'''

    import re

    # Limpio espacios if False y guiones
    if space==True:
        s = string.lower().replace("-", "")
    else:
        s = string.lower().replace(" ", "").replace("-", "")
    
    s = s.replace('á', 'a')
    s = s.replace('é', 'e')
    s = s.replace('í', 'i')
    s = s.replace('ó', 'o')
    s = s.replace('ú', 'u')

    re.sub(r'\W', '', s)

    # clean_nombres(s)
    return s 

def clean_mail(string, space=False):
    '''Limpia un mail eliminando los puntos y el @. Pasa a minúsculas, elimina espacios y caractéres no alfanuméricos.'''

    import re
    s = str(string)
    s = s.lower().replace("-", "")
    # s = s.replace('@gmail.com', '').replace('.', '')

    s = s.replace('á', 'a')
    s = s.replace('é', 'e')
    s = s.replace('í', 'i')
    s = s.replace('ó', 'o')
    s = s.replace('ú', 'u')

    re.sub(r'\W', '', s)

    # clean_nombres(s)
    return s 

def clean_reps(serie, thresh=5):
    '''Limpia palabras repetidas más de [thresh] veces en la serie. Default: 5 repeticiones'''

    import re
    import collections
    import pandas as pd
    import numpy as np

    serie = serie.astype(str)

    if len(serie) <= 1:
        return serie

    def repeticiones(serie_rep, thresh):
        ser_list = pd.Series(serie_rep.unique()).str.split(" ")

        #Appendeo todas las palabras en una lista separada por comas
        lista = []
        for x in ser_list:
            lista += x 

        repeats = pd.value_counts(np.array(lista))
        repeats =repeats[repeats.ge(thresh)].index.str.lower().to_list() #Solo me quedo con las palabras que se repiten más de [thresh] veces
        return repeats 

    reps = repeticiones(serie, thresh)
        
    serie_split = serie.str.split(" ")
    for i in range(len(serie_split)):
        serie_split.iloc[i] = [x for x in serie_split.iloc[i] if x not in reps]
    cleared = serie_split.str.join(" ")

    print("palabras eliminadas:", reps)
    return cleared

def clean_nombres(string):
    from fuzzywuzzy import fuzz

    # # Pongo nan si la string que voy a buscar tiene el nombre de alguna de las personas que simulamos en el mail - encontraría demasiados matchs
    arg = ['gomez','pablo','pablo','gomez','pablo','gomez','pablo','gomez','pablo','gomez','gomez','pablo','rodriguez','manuel','manuel','rodriguez','manuel','rodriguez','manuel','rodriguez','rodriguez','manuel','rodriguez','manuel','María','Laura','Florencia','Belén','Pablo','Manuel','Juan','Luis']
    col = ['carlos','gonzalez','carlos','gonzalez','gonzalez','carlos','carlos','gonzalez','gonzalez','carlos','rodriguez','jose','jose','jose','rodriguez','jose','María','Luz','Ana','Mónica','Carlos','José','Luis','Juan']
    ecu = ['zambrano','luis','zambrano','luis','zambrano','luis','luis','zambrano','garcia','juan','María','Rosa','Ana','Diana','Luis','Juan','José','Carlos']
    per = ['daniel','flores','alejandro','dominguez','Laura','María','Cristina','Marta','Daniel','Alejandro','Carlos','David']
    nombres = list(set(arg+col+ecu+per))

    for nom in nombres:
        ratio = fuzz.ratio(nom.lower(), string)

        if ratio > 90:   
            string=""
            break
    return string


#### Micro Funciones

def matchs_que_faltan(scrap, rtas, show=False):
    '''Genera los dataframe scrap_aux y rtas_aux, que identifican las observaciones no matcheadas anteriormente.'''

    import pandas as pd
    
    scrap_aux = scrap[scrap['index_rtas'].isnull()]    #Duplico el df asi puedo borrar y editar sin problema de perder datos 
    rtas_aux = rtas[rtas['index_scrap'].isnull()]                    #las bases aux identifican observaciones no matcheadas en cada instancia
    target = len(rtas_aux)

    if show:
        print("\nFaltan:", target, "- El", target/len(rtas)*100,"% de las observaciones\n", sep=" ")
    
    return scrap_aux, rtas_aux

def cuenta_matchs(rtas, show=False, only_perc=False):
    '''Devuelve la cantidad / porcentaje de matchs entre respuestas y scrap.'''

    import pandas as pd
    
    faltan = len(rtas[rtas['index_scrap'].isnull()])                              # Target son las que faltan
    total = len(rtas[rtas['body'].notnull()])  
    matcheadas = total - faltan
    try:
        perc = matcheadas / total
        if show==True:
            print("\nEn total se matchearon: ", matcheadas, " - El ", perc,"% de las observaciones\n", sep="")

        if only_perc==True:
            return perc
        
        else:
            return [matcheadas, perc, total]        
        
    except:
        if show==True:
            print("\nSe matchearon todas las observaciones: (", matcheadas, ")", sep="")

        if only_perc==True:
            return 0
        
        else:
            return [matcheadas, 0, total]        


def informe_no_match(rtas, show=False, perc=False, informe_match=False):
    no_matched = rtas[rtas['index_scrap'].isnull()]
    if show==True:
        print("No matcheadas:",no_matched)
    else:            
        return no_matched

### Fuzzy search

def fuzzy_search(rtas, scrap, var_j, var_i, max_s=0, match2_j=None, match2_i=None, max_s_2=1, 
                 report=False, confiab=100, min_size=1, step=None, space=False, bar=False):
    '''Busca matchear las strings de la variable i con la variable j (fuzzy seach - find_near_matches).
        - Permite agregar un segundo criterio - deben cumplirse ambos al mismo tiempo para generar el match.
        - Permite modificar la cantidad de caracteres diferentes (max_l es la distancia Levenshtein - la cantidad de sustituciones o substracciones de caracteres que hay que hacer para encontrar el string). Default: 0 (encontrar el match exacto). 
    '''
    import pandas as pd
    import numpy as np
    from fuzzysearch import find_near_matches
    import sys
    import warnings

    warnings.filterwarnings("ignore")
    pd.options.mode.chained_assignment = None  # default='warn'

    # Descripción de la operación
    if match2_j == None:
        # print(f"{var_j} -> {var_i}, (max sust.: {max_s})")
        pass
    else:
        # print(f"{var_j} -> {var_i} (max sust.: {max_s}) y {match2_j} -> {match2_i}, (distancia max: {max_s_2})")  
        pass
    scrap_aux, rtas_aux = matchs_que_faltan(scrap=scrap, rtas=rtas) # Cuento las observaciones que faltan matchear y limpio aux para que solo contenga esas.

    report = {}

    n = len(rtas_aux) # n es la cantidad de rtas que aún no matcheamos
    i = 0

    while i < n:    # Loopeo para cada respuesta

        j = 0       # Reinicio j, la observacion scrapeada que quiero analizar
        if bar:
            #### Esto es la barra de carga - ignorar
            d = (i + 1) / n
            sys.stdout.write('\r')
            sys.stdout.write("[%-20s] %d%%" % ('='*int(20*d), 100*d))    
            sys.stdout.flush()
            #######

        if len(str(rtas_aux[var_i].iloc[i])) < min_size:              # Si la obs está vacia, no hacer nada...
            i+=1
            continue

        if rtas_aux[var_i].iloc[i] == np.nan:
            i+=1
            continue

        while j<len(scrap_aux):  #Loopeo siempre que: a) no haya buscado en todas las observaciones y b) no haya encontrado un match (linea 50 y 56) 

            if len(str(scrap_aux[var_j].iloc[j])) < min_size:         # Si la obs está vacia, no hacer nada...
                j+=1 
                continue
        
            if scrap_aux[var_j].iloc[j] == np.nan:         # Si la obs está vacia, no hacer nada...
                j+=1 
                continue

            rta_i = clean(str(rtas_aux[var_i].iloc[i]), space=space)         # rtas_aux[var_j].iloc[j] es la observacion j de las rtas no matchadas, considerando únicamente la variable j. Es solamente una string!
            scrap_j = clean(str(scrap_aux[var_j].iloc[j]), space=space)      # clean limpia esa string (ver funcion clean())

            index_i = rtas_aux.iloc[[i]].index[0]               # Acá obtengo los indices de la observacion i y j, para despues identificarlos en las bases originales vía esos indices.
            index_j = scrap_aux.iloc[[j]].index[0]               

            if len(rta_i)<=5:
                break
            if len(scrap_j)<=5:
                break
            
            matches = find_near_matches(scrap_j, rta_i, max_substitutions=max_s, max_insertions=0, max_deletions=0)
            
            if len(matches) > 0:                                 # Si matchea (i.e. matches no es una lista vacía)

                if match2_j==None:                               # Si no hay 2da condicion, siempre se cumple 
                    matches2 = 'nan'

                else:                                            # Si tengo una segunda condición, limpio las strings y hago el match
                    match2_i_clean = clean(str(rtas_aux[match2_i].iloc[i]), space=space)                  
                    match2_j_clean = clean(str(scrap_aux[match2_j].iloc[j]), space=space)
                    matches2 = find_near_matches(match2_j_clean, match2_i_clean, max_substitutions=max_s_2, max_insertions=0, max_deletions=0)

                if len(matches2) > 0:                            # Si matchea

                    scrap['index_rtas'].loc[index_j]= index_i        #Le agrego el indice de la respuesta al scrap original
                    scrap['match_confiab'].loc[index_j]=confiab
                    scrap['match_step'].loc[index_j]=step

                    rtas['index_scrap'].loc[index_i] = index_j                     #Le agrego el id a la base de respuestas
                    rtas['match_confiab'].loc[index_i]=confiab
                    rtas['match_step'].loc[index_i]=step
                    rtas['match_string'].loc[index_i]=matches
                    rtas['match2_string'].loc[index_i]=matches2

                    report[str(index_i)] = [matches, index_j, index_i]
                    break                                        #Si encuentra, salgo del loop de j
                
            j+=1    # Analizo la siguiente observacion del scrap
        
        i+=1   # Analizo la siguiente observacion de las rtas
        

    matchs_que_faltan(scrap=scrap, rtas=rtas) 

    if report==False:
        report = {}
        return report
    return scrap_aux, rtas_aux

def match_string_con_lista(string,lista_matchs):
    ''' A partir de una string, busca matchar con las palabras de la lista.
            - Input: string.
            - Output: bool. True si hay match / False si no hay
    '''

    from fuzzysearch import find_near_matches

    # # Pongo nan si la string que voy a buscar tiene el nombre de alguna de las personas que simulamos en el mail - encontraría demasiados matchs

    matchea = False

    for word in lista_matchs:
        encuentra = find_near_matches(clean(word, space=True), clean(string, space=True), max_l_dist=1)
        if encuentra != []:
            matchea = True
            break
        
    return matchea
    


########### Algoritmos

def limpia_vars(scrap, thresh_name=100, thresh_inmob=100):
        # Creo columna con nombres de inmobiliaria limpios
    print("Limpio nombre de inmobiliarias, precios y calles:")
    scrap['nombre_c'] = clean_reps(scrap['nombre'], thresh=thresh_name)
    scrap['precio_c'] = clean_reps(scrap['precio'], thresh=thresh_name)
    scrap['inmobiliaria_c'] = clean_reps(scrap['inmobiliaria'], thresh=thresh_inmob)
    scrap['inmobiliaria_c'] = scrap['inmobiliaria_c'].apply(clean_nombres)
    print("Despues de la limpieza... ¿hay repetidas?")
    print(scrap['inmobiliaria_c'].duplicated().value_counts())
    
    return scrap

def algoritmo_properati_solo_id(path, rtas, scrap, nombre=""):
    ''' Matchea utilizando solo el id de properati    '''
    import numpy as np
    
    print("Cantidad de publicaciones scrapeadas:", len(scrap),"\nCantidad de respuestas:", len(rtas))
   
    no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:
        print("1) Match por id de properati")
        # Mutear si no anda
        # scrap['id_c'] = scrap['id'].apply(clean) 
        # rtas['body_c'] = rtas['body'].apply(clean)
        #
        fuzzy_search(rtas, scrap,'script_string_3', 'body', max_s=0, confiab=100, step=1, space=False)
        no_matched = len(informe_no_match(rtas, show=False))
        
    informe_no_match(rtas)

    # Exports
    import os
    if os.path.isdir(path)==False:
        os.mkdir(path)

    sent = scrap[scrap['index_rtas'].notnull()]
    out = sent.merge(rtas, how='outer', right_on="index_scrap", left_index=True, indicator=True)
    # out.to_excel(path + "\\sent_" + nombre +".xlsx")
    # print(path + "\\sent_" + nombre +".xlsx creado")
    return out


def algoritmo_properati(path, rtas, scrap, nombre="" ):
    
    # print("Cantidad de publicaciones scrapeadas:", len(scrap),"\nCantidad de respuestas:", len(rtas))

    # print("\nVerifico si las inmobiliarias se repiten (True=repetidas, False=unicas)")
    # print(scrap['inmobiliaria'].duplicated().value_counts())
    
    no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:
        # print("1) Match por id de properati")
        fuzzy_search(rtas, scrap,'id', 'body', max_s=0, confiab=100, step=1)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:
        # print("2) Match por descripcion")
        var = [col for col in scrap if col.startswith('descripcion')]
        fuzzy_search(rtas, scrap,var[0], 'body', max_s=3, confiab=100, step=2)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        # print("3) Match por nombre de inmobiliaria full")
        fuzzy_search(rtas, scrap, 'inmobiliaria', 'from', max_s=0, confiab=90, step=3.1, space=True)
        fuzzy_search(rtas, scrap, 'inmobiliaria', 'from', max_s=1, confiab=80, step=3.2, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'body', max_s=0, confiab=90, step=3.3, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'body', max_s=1, confiab=80, step=3.4, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'subject', max_s=0, confiab=90, step=3.5, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'subject', max_s=1, confiab=80, step=3.6, space=True)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        # print("4) Match por nombre de inmobiliaria limpia")  # Matcheo limpio, pero los que tienen que tener al menos 5 letras
        # Sin remover espacios
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'subject', 0, confiab=90, min_size=1, step=4.1, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'body', 0, confiab=90, min_size=1, step=4.2, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'body', 0, confiab=90, min_size=5, step=4.3, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'subject', 1, confiab=70, min_size=5, step=4.4, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'from', 1, confiab=70, min_size=5, step=4.5)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'body', 1, confiab=70, min_size=5, step=4.6)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        # print("5) Match por calle + precio")
        fuzzy_search(rtas, scrap,'nombre', 'body', 2,'precio', 'body', max_s_2=1, step=5)
        
    informe_no_match(rtas)

    # Exports
    import os
    if os.path.isdir(path)==False:
        os.mkdir(path)

    # rtas.to_excel(path + "\\" + nombre + "_respuestas_matched.xlsx")
    # print(nombre + "_respuestas_matched.xlsx creado")
    # scrap.to_excel(path + "\\" + nombre +"_scrap_matched.xlsx")
    # print(nombre + "_scrap_matched.xlsx creado")

    # rtas.set_index('index_scrap', inplace=True)
    # scrap.set_index('index_rtas', inplace=True)
    out = scrap.merge(rtas, how='outer', right_on="index_scrap", left_index=True, suffixes=('_conf', '_inmob'))
    # print(nombre)
    nombre = nombre.replace("@gmail.com","").replace("@gmailcom","")
    out.to_excel(path + "\\" + nombre + "_matched.xlsx")
    # print(path + "\\" + nombre + "_matched.xlsx creado")
    return out

def algoritmo_zonaprop(path, rtas, scrap, nombre=""):
    
    print("Cantidad de publicaciones scrapeadas:", len(scrap),"\nCantidad de respuestas:", len(rtas))

    print("\nVerifico si las inmobiliarias se repiten (True=repetidas, False=unicas)")
    print(scrap['inmobiliaria'].duplicated().value_counts())
    
    no_matched = len(informe_no_match(rtas, show=False))
    print(no_matched)
    if no_matched>0:
        print("1) Match por id de properati")
        fuzzy_search(rtas, scrap,'cod_zonaprop', 'body', max_s=0, confiab=100, step=1)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:
        print("2) Match por descripcion")
        var = [col for col in scrap if col.startswith('descripcion')]
        fuzzy_search(rtas, scrap,var[0], 'body', max_s=3, confiab=100, step=2)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        print("3) Match por nombre de inmobiliaria full")
        fuzzy_search(rtas, scrap,'inmobiliaria', 'from', max_s=0, confiab=90, step=3.1, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'from', max_s=1, confiab=80, step=3.2, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'body', max_s=0, confiab=90, step=3.3, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'body', max_s=1, confiab=80, step=3.4, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'subject', max_s=0, confiab=90, step=3.5, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria', 'subject', max_s=1, confiab=80, step=3.6, space=True)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        print("4) Match por nombre de inmobiliaria limpia")  # Matcheo limpio, pero los que tienen que tener al menos 5 letras
        # Sin remover espacios
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'subject', 0, confiab=90, min_size=1, step=4.1, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'body', 0, confiab=90, min_size=1, step=4.2, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'from', 0, confiab=90, min_size=5, step=4.3, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'subject', 1, confiab=70, min_size=5, step=4.4, space=True)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'body', 1, confiab=70, min_size=5, step=4.5)
        fuzzy_search(rtas, scrap,'inmobiliaria_c', 'from', 1, confiab=70, min_size=5, step=4.6)
        no_matched = len(informe_no_match(rtas, show=False))

    if no_matched>0:   
        print("5) Match por calle + precio")
        fuzzy_search(rtas, scrap,'direccion2', 'body', 2,'precio', 'body', max_s_2=1, step=5)
        
    informe_no_match(rtas)

    # Exports
    import os
    if os.path.isdir(path)==False:
        os.mkdir(path)

    # rtas.to_excel(path + "\\" + nombre + "_respuestas_matched.xlsx")
    # print(nombre + "_respuestas_matched.xlsx creado")
    # scrap.to_excel(path + "\\" + nombre +"_scrap_matched.xlsx")
    # print(nombre + "_scrap_matched.xlsx creado")

    # rtas.set_index('index_scrap', inplace=True)
    # scrap.set_index('index_rtas', inplace=True)
    out = scrap.merge(rtas, how='outer', right_on="index_scrap", left_index=True)
    out.to_excel(path + "\\" + nombre + "_matched.xlsx")
    nombre=nombre.replace("@gmail.com","")
    print(path + "\\" + nombre + "_matched.xlsx creado")

def full_proceso(rtas_full, scrap_full, path, web='properati'):

    import numpy as np
    import pandas as pd
    import os
    from datetime import datetime
    from tqdm import tqdm
    
    today = datetime.now()
    today = today.strftime("%Y_%m_%d")

    print("\n###########################################################")
    print("Cantidad de Respuestas:", len(rtas_full), "\n###########################################################")

    # Limpio repeticiones
    scrap = limpia_vars(scrap_full) 

    # Spliteo por todos los mails que mandamos
    mails = pd.unique(list(scrap.mail.apply(clean_mail))).tolist()

    # Loopeo por cada uno de los perfiles de personas que "mandaron mails"
    i=0
    full_matcheados = pd.DataFrame()
    reporte_matchs = {}

    for mail in tqdm(mails):
        
        # print("\n###########################################################")

        # Creo los df rtas y scrap, que uso para matchear
        rtas = rtas_full[rtas_full['to'].apply(clean_mail)==mail].reset_index(drop=True)
        scrap = scrap_full[scrap_full['mail'].apply(clean_mail)==mail].reset_index(drop=True)
        # print("Analisis de respuestas de"+str(mail))

        # Creo los index, confiab y aux que uso para matchear
        rtas['index_scrap'] = np.nan
        rtas['match_confiab'] = np.nan
        rtas['match_step'] = np.nan
        rtas['match_string'] = np.nan
        rtas['match2_string'] = np.nan
        scrap['index_rtas'] = np.nan
        scrap['match_confiab'] = np.nan
        scrap['match_step'] = np.nan

        scrap_aux = scrap.copy()     #Duplico el df asi puedo borrar y editar sin problema de perder datos 
        rtas_aux = rtas.copy()       #las bases aux identifican observaciones no matcheadas en cada instancia 

        # if web=='zonaprop':
        #     algoritmo_zonaprop(path= path, nombre=fr"\{name}_piloto", rtas=rtas, scrap=scrap)
        
        if not os.path.exists(path + '\sueltas'):
            os.makedirs(path + '\sueltas')

        if web=='properati':
            matcheados = algoritmo_properati(path= path, nombre=fr"\sueltas\respuestas_inmobiliarias_{today}_{mail}", rtas=rtas, scrap=scrap)

        full_matcheados = full_matcheados.append(matcheados)
        reporte_matchs[str(mail)] = cuenta_matchs(rtas)
        
        i += 1

    df = pd.DataFrame.from_dict(reporte_matchs,orient='index',columns=['Matchs','Porcentaje','Total'])

    today = datetime.now()
    today = today.strftime("%Y_%m_%d")
    nombre = 'resultados_matching_rtas_' + today

    df.to_excel(path + fr"\{nombre}.xlsx")
    print(path + fr"\{nombre}.xlsx creado")
    full_matcheados.to_excel(path + fr"\respuestas_inmobiliarias_{today}_matched.xlsx")
    print(path + fr"\respuestas_inmobiliarias_{today}_matched.xlsx creado")
    print("end of script")
    return df

def full_proceso_solo_id(path, rtas_full, scrap_full, pais):
    ''' Matchea por id de properati comparando unicamente mails y publicaciones que coincidan en el mail (el sender == receiver)   '''
    import numpy as np
    import pandas as pd
    import os
    from datetime import datetime
    today = datetime.now()
    today = today.strftime("%Y_%m_%d")

    # Spliteo por todos los mails que mandamos
    mails = scrap_full.mail.unique()
    print(mails)

    # Loopeo por cada uno de los mails de los que se enviaron
    i=0
    full_matcheados = []
    reporte_matchs = {}
    for mail in mails:
        
        print("\n###########################################################")

        # Creo los df rtas y scrap, que uso para matchear
        rtas = rtas_full[rtas_full.to==mail].reset_index(drop=True)
        scrap = scrap_full[scrap_full.mail==mail].reset_index(drop=True)

        print("Analisis de respuestas de "+str(mail))
        matcheados = algoritmo_properati_solo_id(path= path, nombre=fr"\sueltas\conf_envio_{pais}_{today}_{mail}", rtas=rtas, scrap=scrap)

        full_matcheados += [matcheados]
        reporte_matchs[str(mail)] = cuenta_matchs(rtas)
        
        i += 1

    out = pd.concat(full_matcheados)
    reporte = pd.DataFrame.from_dict(reporte_matchs,orient='index',columns=['Matchs','Porcentaje','Total'])
    reporte.to_excel(path + fr"\resultados_matching_conf_envio_" + today + ".xlsx")
    out.to_excel(path + fr"\conf_envio_{today}_matched.xlsx")
    print("end of script")
    return out