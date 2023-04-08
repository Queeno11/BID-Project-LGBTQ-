*CODIGO PARA LIMPIAR BASE DE DATOS FINAL

// local path = "C:\Users\37615965\Dropbox\BID-LGBTQ+\campo\data\para_analisis"  
// local path = "C:\Users\Usuario\Dropbox\BID-LGBTQ+\campo\data\para_analisis"
*local path = "C:\Users\Nico\Dropbox\BID-LGBTQ+\campo\data\para_analisis"
// local path = "C:\Users\Usuario\Dropbox\BID-LGBTQ+\campo\data\para_analisis"
// local path = "E:\Dropbox\BID-LGBTQ+\campo\data\para_analisis"
local path = "D:\Dropbox\BID-LGBTQ+\campo\data\para_analisis"

cd `path'

use "base_limpia_tarea_2_nico", clear

gen to_inmob_2 = subinstr(to_inmob, ".", "", .)

replace to_inmob_2 = "" if to_inmob_2 == "nan"

merge m:1 to_inmob_2 ronda using "treatments_by_round_complete", keepusing(treatment_2 ses_2 male_name_2)

drop if _merge == 2

drop _merge

replace treatment = treatment_2 if treatment == "nan" & tipo == "solo_respuesta"

replace ses = ses_2 if ses == "nan" & tipo == "solo_respuesta"

replace male_name = male_name_2 if male_name == "nan" & tipo == "solo_respuesta"

drop treatment_2 ses_2 male_name_2 to_inmob_2

replace treatment = "" if treatment == "nan"

replace ses = "" if ses == "nan"

*Renombramos y etiquetamos el resto de las variables 

rename ronda round

lab var round "Message sending round number"

rename  id_publicacion id_number 

lab var id_number "Unique advertisement identification code (number)"

order id_number, after(id)

replace tipo = "" if tipo == "nan"

encode tipo, gen(obs_type)

order obs_type, after(tipo)

drop tipo

lab drop obs_type

lab def obs_type 1 "Advertisement matched with response" 2 "Just advertisement" 3 "Just response"

lab var obs_type "Type of observation"

lab val obs_type obs_type

rename inmobiliaria real_estate

replace real_estate = "" if real_estate == "nan"

lab var real_estate "Real estate agency"

rename fecha date_ad

lab var date_ad "Advertisement date"

lab var link "Link to the advertisement"

replace link = "" if link == "nan"

rename municipio district 

replace district = "" if district == "nan"

encode district, gen(district_2)

order district_2, after(district)

rename district district_aux

rename district_2 district

lab var district "District name"

replace ciudad = "" if ciudad == "nan"

encode ciudad, gen(city)

order city, after(ciudad)

drop ciudad

lab var city "Metropolitan area"

lab drop city

lab def city 1 "Greater Arequipa" 2 "Greater Bogota" 3 "Greater Buenos Aires" 4 "Greater Guayaquil" 5 "Greater Lima" 6 "Greater Medellin" 7 "Greater Quito" 8 "Greater Rosario"

lab val city city

order pais, after(city)

replace pais = "" if pais == "nan"

rename pais country

drop pais*

encode country, gen(country_2)

order country_2, after(country)

drop country

rename country_2 country


lab var country "Country"

rename descripcion description 

replace description = "" if description == "nan"

lab var description "Description of the apartment in the advertisement"

rename precio price 

lab var price "Apartment rental price"

rename moneda currency

replace currency = "" if currency == "nan"

lab var currency "Currency in which the price is expressed"

rename ambientes rooms 

lab var rooms "Number of rooms in the apartment"

rename habitaciones bedrooms 

lab var bedrooms "Number of bedrooms in the apartment"

rename banos bathrooms 

lab var bathrooms "Number of bathrooms in the apartment"

rename m2_totales total_m2

lab var total_m2 "Total square meters"

rename m2_cubiertos covered_m2

lab var covered_m2 "Covered square meters"

rename cochera garage 

lab var garage "Number of garages available"

replace id = "" if id == "nan"

lab var id "Unique advertisement identification code"

rename precio_local price_dom

lab var price_dom "Rental price expressed in domestic currency"

lab var p_m2 "Price per square meter"

lab var p_m2_above_mdn "1 = Price per square meter above median"

lab var m2_above_mdn "1 = Square meters above median"

lab var n_adds "Number of advertisements published by real estate"

lab var n_adds_above_mdn "Number of advertisements by real estate above median"

lab var common_name "Number of repeated real estate agencies across districts"

lab var capital "1 = district is country's capital city"

encode treatment, gen(treatment_2)

order treatment_2, after(treatment)

drop treatment

rename treatment_2 treatment

lab var treatment "Treatment assigned to real estate agency"

encode ses, gen(ses_2)

order ses_2, after(ses)

drop ses

rename ses_2 ses

lab var ses "Socio-economic status revealed to the real estate agency"

replace male_name = "" if male_name == "nan"

lab var male_name "Name with which the message is signed"

replace male_couple = "" if male_couple == "nan"

lab var male_couple "Couple's name (used for homosexual couples)"

replace female_couple = "" if female_couple == "nan"

lab var female_couple "Couple's name (used for heterosexual and trans couples)"

lab var script "Assigned message script"

replace script_string = "" if script_string == "nan"

lab var script_string "Assigned message script text"

replace script_string_2 = "" if script_string_2 == "nan"

lab var script_string_2 "Names of the signatories used for string 2"

rename nombre_2 title_ad 

replace title_ad = "" if title_ad == "nan"

lab var title_ad "Advertisement's title'"

rename n_adds_muni n_adds_dist 

lab var n_adds_dist "Number of advertisements published by real estate-district"

rename n_adds_muni_above_mdn n_adds_dist_above_mdn

lab var n_adds_dist_above_mdn "Number of advertisements by real estate-district above median"

rename mail email

replace email = to_inmob if email == ""

replace email = "" if email == "nan"

lab var email "Email account that received the response"

drop randomization_seed

lab var subject_conf "Subject of the sending confirmation email"

lab var body_conf "Body of the sending confirmation email"

lab var date_conf "Date of the sending confirmation email"

lab var date_conf_clean "Clean date of the sending confirmation email"

order date_conf_clean, after(date_conf)

drop nombre match_confiab_conf match_step_conf nombre_c precio_c inmobiliaria_c mimeType_inmob match_step_inmob match_string_inmob match2_string_inmob date_clean_inmob date_clean_conf  random  from_conf to_conf id_merge

drop id_pub2

drop cc*

rename subject_inmob subject_response

replace subject_response = "" if subject_response == "nan"

lab var subject_response "Subject of the response email"

rename from_inmob sender_response

replace sender_response = "" if sender_response == "nan"

lab var sender_response "Sender of the response email"

rename to_inmob rec_response

replace rec_response = "" if rec_response == "nan"

lab var rec_response "Receiver of the response email"

rename body_inmob body_response

replace body_response = "" if body_response == "nan"

lab var body_response "Body of the response email"

rename date_inmob date_response

lab var date_response "Date of the response email"


rename date_inmob_clean date_clean_response

lab var date_clean_response "Clean date of the response email"

rename id_respuesta id_response 

lab var id_response "Unique response identification code"

replace mail_number = . if mail_number < 0 

lab var mail_number "Order of received emails by real estate"

rename match_confiab_inmob match_reliab_response

lab var match_reliab_response "Matching reliability index"

lab var time_diff "Time difference beetween confirmation and response (in minutes)"

rename categoria category

lab var category "RA Response classification"

lab def category 1 "No response" 2 "Automatic response" 3 "Not available" 4 "Not available, alternative is offered" 5 "Not available, revalued" 6 "Available, more info is provided" 7 "Available, possibility of visit" 8 "Available, arrange visit" 99 "Unclear"

lab val category category

rename colateral collateral

lab var collateral "RA. Real estate asks for collateral"

rename telefono phone

lab var phone "RA. Real estate offers phone number for further contact"

rename automatico automatic

lab var automatic "RA automatic response classification"

rename noes not_valid

lab var not_valid "RA. Response not valid for analysis"

rename observacion ra_notes

replace ra_notes = "" if ra_notes == "nan"

lab var ra_notes "RA notes on response"

rename ra ra_name

replace ra_name = "" if ra_name == "nan"

lab var ra_name "RA name"

rename RA ra_match_name

replace ra_match_name = "" if ra_match_name == "nan"

lab var ra_match_name "RA_match. Name"

cap rename Nom_Inmobiliaria real_estate_name

cap replace real_estate_name = "" if real_estate_name == "nan"

cap lab var real_estate_name "RA_match. Real estate's name identified by RA"

cap rename Comentario match_comments

cap replace match_comments = "" if match_comments == "nan"

cap lab var match_comments "RA_match. RA Comment on match"

rename match_manual manual_match 

lab var manual_match "1 = Manually matched response"

// lab var match_david "1 = Manually matched by David" # FIXME: crear esta variable nuevamente

drop subject_conf body_conf  // Esto es el contenido de los mails de confirmacion, que son todos iguales
drop date_conf date_response // Estas son fechas que están en formato string, ya están bien formateadas en _clean!

*Variables adicionales

egen id_real_est = group(real_estate district city) /*Codigo unico por inmobiliaria-municipio*/

lab var id_real_est "Unique identification code by real estate agency"

order id_real_est, after(real_estate)

**Generamos codigos unicos para respuestas matcheadas y no matcheadas 

*1) Respuestas no matcheadas

encode sender_response if obs_type == 3, gen(id_not_matched_response) /*Codigo unico por respuesta no matcheada */

label drop id_not_matched_response

egen id_not_matched_response_2 = concat(id_not_matched_response round) /*Concatenamos este ID con la ronda*/ 

replace id_not_matched_response_2 = "" if obs_type != 3 /*Reemplazamos por missings en las observaciones que no sean respuestas no matcheadas*/

drop id_not_matched_response 

rename id_not_matched_response_2 id_not_matched_response

destring id_not_matched_response, replace 

lab var id_not_matched_response "Unique identification code by sender of not matched response"

gen	not_matched_response = 0 if id_not_matched_response != .

bysort id_not_matched_response: replace	not_matched_response = 1 if _n == 1 & id_not_matched_response != .

lab var not_matched_response "1 = unique observation by sender of not matched response"

*2) Respuestas matcheadas

encode sender_response if obs_type == 1, gen(id_matched_response) /*Codigo unico por respuesta no matcheada */

label drop id_matched_response

egen id_matched_response_2 = concat(id_matched_response round) /*Concatenamos este ID con la ronda*/ 

replace id_matched_response_2 = "" if obs_type != 1 /*Reemplazamos por missings en las observaciones que no sean respuestas no matcheadas*/

drop id_matched_response 

rename id_matched_response_2 id_matched_response

destring id_matched_response, replace 

lab var id_matched_response "Unique identification code by sender of matched response"

gen	matched_response = 0 if id_matched_response != .

bysort id_matched_response: replace	matched_response = 1 if _n == 1 & id_matched_response != .

lab var matched_response "1 = unique observation by sender of matched response"

sort round obs_type id_not_matched_response id_matched_response

/*
Rellenar manualmente las observaciones que tienen valores faltantes en la variable treatment en la ronda 1 
(consultar base auxiliar "RA_not_matched_06_24_2022" en la ruta "Dropbox\BID-LGBTQ+\campo\data\para_analisis\bases auxiliares")
*/

gen sender_response_aux = substr(sender_response, 2, 20)

replace rec_response = "jcmanuel.rodriguez@gmail.com" if sender_response_aux == "Administracion Porta" & round == 1 & treatment == . 
replace rec_response = "r.joserodriguezt@gmail.com" if sender_response_aux == "conkinmobiliaria.inm" & round == 1 & treatment == . 
replace rec_response = "p.h.pablo.gomez@gmail.com" if sender_response == "Adriana Villanueva <adrianavvillanueva@gmail.com>" & round == 1 & treatment == . 
replace rec_response = "ph.pablo.gomez@gmail.com" if sender_response == "Agostina Maroni <amaroni@remax.com.ar>" & round == 1 & treatment == . 
replace rec_response = "jp.gonzalezcarlos@gmail.com" if sender_response == "BCC inmobiliaria <buscocasacol@gmail.com>" & round == 1 & treatment == . 
replace rec_response = "zambrano.k.luis@gmail.com" if sender_response == "Innova Real State Norte <innovarealstate.norte@gmail.com>" & round == 1 & treatment == . 
replace rec_response = "rodriguezjc.manuel@gmail.com" if sender_response == "Lorena Gonzalez Ferioli <lore@personalbroker.biz>" & round == 1 & treatment == . 
replace rec_response = "floresc.danielp@gmail.com" if sender_response == "Pablo Valdivia Cabrera <info@dhinm.com>" & round == 1 & treatment == . 
replace rec_response = "manuelrodriguezjf@gmail.com" if sender_response ==  "Roxana Pena <rochupena@gmail.com>" & round == 1 & treatment == . 
replace rec_response = "jdaniel.floresj@gmail.com" if sender_response == "Tu Inmobiliaria <infotuinmobiliaria.pe@gmail.com>" & round == 1 & treatment == . 
replace rec_response = "carlosmz.gonzalez@gmail.com" if sender_response == "eycbienesraices <eycbienesraices@gmail.com>" & round == 1 & treatment == . 

drop sender_response_aux

replace treatment = 2 if rec_response == "jcmanuel.rodriguez@gmail.com" & round == 1 & treatment == . 
replace treatment = 3 if rec_response == "r.joserodriguezt@gmail.com" & round == 1 & treatment == . 
replace treatment = 1 if rec_response == "ph.pablo.gomez@gmail.com" & round == 1 & treatment == . 
replace treatment = 1 if rec_response == "p.h.pablo.gomez@gmail.com" & round == 1 & treatment == . 
replace treatment = 2 if rec_response == "jp.gonzalezcarlos@gmail.com" & round == 1 & treatment == . 
replace treatment = 2 if rec_response == "zambrano.k.luis@gmail.com" & round == 1 & treatment == . 
replace treatment = 3 if rec_response == "rodriguezjc.manuel@gmail.com" & round == 1 & treatment == . 
replace treatment = 1 if rec_response == "floresc.danielp@gmail.com" & round == 1 & treatment == . 
replace treatment = 2 if rec_response == "manuelrodriguezjf@gmail.com" & round == 1 & treatment == . 
replace treatment = 2 if rec_response == "jdaniel.floresj@gmail.com" & round == 1 & treatment == . 
replace treatment = 1 if rec_response == "carlosmz.gonzalez@gmail.com" & round == 1 & treatment == . 

replace ses = 2 if rec_response == "jcmanuel.rodriguez@gmail.com" & round == 1 
replace ses = 2 if rec_response == "r.joserodriguezt@gmail.com" & round == 1 
replace ses = 2 if rec_response == "p.h.pablo.gomez@gmail.com" & round == 1
replace ses = 1 if rec_response == "jp.gonzalezcarlos@gmail.com" & round == 1 
replace ses = 1 if rec_response == "zambrano.k.luis@gmail.com" & round == 1 
replace ses = 1 if rec_response == "rodriguezjc.manuel@gmail.com" & round == 1 
replace ses = 1 if rec_response == "floresc.danielp@gmail.com" & round == 1 
replace ses = 1 if rec_response == "manuelrodriguezjf@gmail.com" & round == 1 
replace ses = 2 if rec_response == "jdaniel.floresj@gmail.com" & round == 1 
replace ses = 2 if rec_response == "carlosmz.gonzalez@gmail.com" & round == 1 

*Rellenar manualmente las observaciones de la variable pais con datos faltantes



* Identificador unico inmobiliaria:

tempfile df_original
save `df_original'
bysort real_estate district round: gen aux = _n

keep if aux == 1
drop if real_estate == ""

preserve
keep if round == 2
keep real_estate district
tempfile round_2
save `round_2'
restore

keep if round == 1
keep real_estate district
merge 1:1 real_estate district using `round_2', nogen keep(3)
egen id_inmob_unique = group(real_estate district)

merge 1:m real_estate district using `df_original', nogen keep(2 3)

gen only_round = 1 if id_inmob_unique == . & round == 1
replace only_round = 2 if id_inmob_unique == . & round == 2
replace only_round = 3 if id_inmob_unique != .


label var only_round "=1 only round 1, =2 only round 2, =3 both rounds."
label var id_inmob_unique "Unique real estate identifier in both rounds."

*Corregimos la variable de ID por inmobiliaria para las observaciones no matcheadas

replace id_inmobiliaria = "" if obs_type == 3 

*Asignamos valores a las observaciones que tienen missing en pais

replace country = 1 if city == 3 | city == 8

replace country = 2 if city == 2 | city == 6

replace country = 3 if city == 4 | city == 7

replace country = 4 if city == 1 | city == 5 

*Rellenar manualmente las observaciones de la variable pais con datos faltantes

gen rec_response_aux = rec_response

replace rec_response_aux = subinstr(rec_response, ".", "",  10)

replace country = 4 if rec_response_aux == "bdominguezalejandroc@gmailcom"
replace country = 4 if rec_response_aux == "bfloresdanielj@gmailcom"
replace country = 2 if rec_response_aux == "carlosmzgonzalez@gmailcom"
replace country = 4 if rec_response_aux == "fdominguezalejandrog@gmailcom"
replace country = 1 if rec_response_aux == "fhpablogomez@gmailcom"
replace country = 1 if rec_response_aux == "gomezfhpablo@gmailcom"
replace country = 1 if rec_response_aux == "gomezpablou@gmailcom"
replace country = 1 if rec_response_aux == "jcmanuelrodriguez@gmailcom"
replace country = 4 if rec_response_aux == "jdanielfloresj@gmailcom"
replace country = 1 if rec_response_aux == "jfmanuelrodriguez@gmailcom"
replace country = 2 if rec_response_aux == "josedrodriguezw@gmailcom"
replace country = 2 if rec_response_aux == "josefrodriguezu@gmailcom"
replace country = 1 if rec_response_aux == "pablogomezfh@gmailcom"
replace country = 1 if rec_response_aux == "pablomngomez@gmailcom"
replace country = 1 if rec_response_aux == "phpablogomez@gmailcom"
replace country = 2 if rec_response_aux == "rjoserodriguezt@gmailcom"
replace country = 1 if rec_response_aux == "rodriguezjcmanuel@gmailcom"
replace country = 2 if rec_response_aux == "rodriguezljosec@gmailcom"
replace country = 1 if rec_response_aux == "rodriguezmanueljf@gmailcom"
replace country = 1 if rec_response_aux == "rodriguezmanuelw@gmailcom"
replace country = 2 if rec_response_aux == "rodriguezmjosev@gmailcom"
replace country = 2 if rec_response_aux == "scarlosgonzalezm@gmailcom"
replace country = 2 if rec_response_aux == "wgonzalezcarlosp@gmailcom"
replace country = 2 if rec_response_aux == "wjoserodriguezp@gmailcom"
replace country = 3 if rec_response_aux == "zambranofluis@gmailcom"
replace country = 3 if rec_response_aux == "zambranokluis@gmailcom"
replace country = 3 if rec_response_aux == "zambranoluispt@gmailcom"

drop rec_response_aux 


gen 	country_2 = "ARG" if country == 1
replace country_2 = "COL" if country == 2
replace country_2 = "ECU" if country == 3
replace country_2 = "PER" if country == 4

tempfile original 
save `original', replace

* Incoporamos el número de inmuebles scrappeados x municipio y el número de inmobiliarias x municipio.
clear
gen district_aux = ""
tempfile num_property
save `num_property'

tempfile num_prop_manager
save `num_prop_manager'

* Ronda 1:
// local path_data_r1 = "E:\Dropbox\BID-LGBTQ+\campo\data\scrape\clean\round1" /*Ruta Juli*/
local path_data_r1 = "D:\Dropbox\BID-LGBTQ+\campo\data\scrape\clean\round1" /*Ruta Juli*/
// local path_data_r1 = "C:\Users\Usuario\Dropbox\BID-LGBTQ+\campo\data\scrape\clean\round1" /*Ruta Luis*/

local countries = "scrape_clean_ARG_v2_2022_04_19 scrape_clean_COL_2022_04_19 scrape_clean_ECU_2022_04_19 scrape_clean_PER_2022_04_19"


foreach c of local countries {
	import excel "`path_data_r1'/`c'.xlsx", sheet("Sheet1") firstrow clear
	* Dropeamos duplicados de links:
	bysort link: gen aux = _n
	drop if aux == 2
	drop aux
	* Generamos ronda:
	gen round = 1
	
	* Variable(s) para merge:
	rename municipio district_aux
	drop if district_aux == ""
	rename pais country_2
	
	* Limpieza strings:
	// include "E:\Dropbox\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do" /*Ruta Juli*/
	include "D:\Dropbox\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do"
	// include "C:\Users\Usuario\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do" /*Ruta Luis*/

	limpieza_strings, varlist(district_aux inmobiliaria)

	gen aux = 1

	* Número de inmuebles scrappeados x municipio 
	preserve
	collapse (sum)aux, by(round district_aux country_2)
	rename aux num_property
	append using `num_property'
	save `num_property', replace
	restore
	
	* El número de inmobiliarias x municipio. 
	preserve
	bysort inmobiliaria district_aux: gen aux2 = _n
	replace aux2 = 0 if aux2 > 1
	collapse (sum)aux2, by(round district_aux country_2)
	rename aux2 num_prop_manager
	append using `num_prop_manager'
	save `num_prop_manager', replace
	restore

}


* Ronda 2:
*local path_data_r2 = "E:\Dropbox\BID-LGBTQ+\campo\data\scrape\clean" /*Ruta Juli*/
local path_data_r2 = "D:\Dropbox\BID-LGBTQ+\campo\data\scrape\clean" /*Ruta Juli*/
*local path_data_r2 = "C:\Users\Usuario\Dropbox\BID-LGBTQ+\campo\data\scrape\clean" /*Ruta Luis*/
local countries = "scrape_clean_ARG_2022_05_20_temp scrape_clean_COL_2022_05_03 scrape_clean_ECU_2022_05_03 scrape_clean_PER_2022_05_03"

foreach c of local countries {
	import excel "`path_data_r2'/`c'.xlsx", sheet("Sheet1") firstrow clear
	* Dropeamos duplicados de links:
	bysort link: gen aux = _n
	drop if aux == 2
	drop aux
	* Generamos ronda:
	gen round = 2
	
	* Variable(s) para merge:
	rename municipio district_aux
	drop if district_aux == ""
	rename pais country_2
	
	* Limpieza strings:
	*include "E:\Dropbox\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do" /*Ruta Juli*/
	include "D:\Dropbox\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do" /*Ruta Juli*/
	// include "C:\Users\Usuario\BID-LGBTQ+\campo\code\pipeline (matches + analisis)\limpieza_strings.do" /*Ruta Luis*/
	limpieza_strings, varlist(district_aux inmobiliaria)

	gen aux = 1
	* Número de inmuebles scrappeados x municipio 
	preserve
	collapse (sum)aux, by(round district_aux country_2)
	rename aux num_property
	append using `num_property'
	save `num_property', replace
	restore
	
	* El número de inmobiliarias x municipio. 
	preserve
	bysort inmobiliaria district_aux: gen aux2 = _n
	replace aux2 = 0 if aux2 > 1
	collapse (sum)aux2, by(round district_aux country_2)
	rename aux2 num_prop_manager
	append using `num_prop_manager'
	save `num_prop_manager', replace
	restore
}


use `num_property', replace

* facatativa (no existe ronda 1) y fray_luis_beltran (no existe en ninguna ronda)
* puente piedra (no existe ronda 2) y lurigancho (no existe ronda 2)
* _merge == 2 y luego las == 1 son las obs_type == just response. Esta OK.
merge 1:m round district_aux country_2 using  `original', keep(2 3) nogen
merge m:1 round district_aux country_2 using  `num_prop_manager', keep(1 3) nogen 

drop district_aux country_2

label var num_prop_manager "Number of property managers"
label var num_property "Number of properties scraped"

* Tarea 17-08 (mensajes sin categorías):
* 378 missings que tenemos en nueva base tarea_nocategorizado_1708.xlsx:
ta category if obs_type != 2, m
* Merge con nuevas categorias:
preserve
import excel "D:\Dropbox\BID-LGBTQ+\campo\data\RA\tarea_nocategorizado_1708\enviado\tarea_nocategorizado_1708.xlsx", firstrow clear
* Nos quedamos las relevantes:
keep id_response category collateral phone automatic name not_valid	ra_notes ra_name
* Las denominamos así para tener ambas y hacer chequeos:
rename * *_2
* Para el merge:
rename id_response_2 id_response
tempfile tarea_17_08
isid id_response
save `tarea_17_08'
restore

* Merge con nuevas categorias (no missings): 1. Las que ya tenian categoria, 3. Las nuevas categorizadas.
merge m:1 id_response using `tarea_17_08', assert(1 3) nogen
* Reemplazamos hacia el valor original:
foreach var in category collateral phone automatic name not_valid ra_notes ra_name {
	capture replace `var' = `var'_2 if `var' == .
	capture replace `var' = `var'_2 if `var' == ""
	drop `var'_2
}
* Los just_advertisement (obs_type == 2) no tienen categoria:
assert category != . if obs_type != 2
* Los just_response (obs_type == 3) no tienen pais ni distrito:
assert country != .  if obs_type != 3
assert district != . if obs_type != 3


*Guardamos la base final
save "`path'\base_limpia_tarea_3_luis", replace

