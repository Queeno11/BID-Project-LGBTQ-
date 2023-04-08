capture program drop limpieza_strings
program limpieza_strings
 	* Syntax:
syntax,  varlist(string)
qui {
	foreach var in `varlist' {
			replace `var' = lower(`var')
			replace `var' = usubinstr(`var', "é", "e",.)
			replace `var' = usubinstr(`var', "á", "a",.)
			replace `var' = usubinstr(`var', "ä", "a",.)
			replace `var' = usubinstr(`var', "í", "i",.)
			replace `var' = usubinstr(`var', "ó", "o",.)
			replace `var' = usubinstr(`var', "ö", "o",.)
			replace `var' = usubinstr(`var', "ő", "o",.)
			replace `var' = usubinstr(`var', "Ö", "o",.)
			replace `var' = usubinstr(`var', "ô", "o",.)
			replace `var' = usubinstr(`var', "ø", "o",.)
			replace `var' = usubinstr(`var', "Ü", "u",.)
			replace `var' = usubinstr(`var', "ü", "u",.)
			replace `var' = usubinstr(`var', "ú", "u",.)
			replace `var' = usubinstr(`var', "ã", "a",.)
			replace `var' = usubinstr(`var', "ñ", "n",.)
			replace `var' = usubinstr(`var', "Ñ", "n",.)
			replace `var' = usubinstr(`var', "ç", "c",.)
			replace `var' = usubinstr(`var', "Ø", "o",.)
			replace `var' = usubinstr(`var', "ñ", "n",.)
			replace `var' = usubinstr(`var', "É", "e",.)
			replace `var' = usubinstr(`var', "Á", "a",.)
			replace `var' = usubinstr(`var', "Í", "i",.)
			replace `var' = usubinstr(`var', "Ó", "o",.)
			replace `var' = usubinstr(`var', "Ú", "u",.)
			replace `var' = usubinstr(`var', "è", "e",.)
			replace `var' = usubinstr(`var', "à", "a",.)
			replace `var' = usubinstr(`var', "ì", "i",.)
			replace `var' = usubinstr(`var', "ò", "o",.)
			replace `var' = usubinstr(`var', "ù", "u",.)
			replace `var' = subinstr(`var', `"""', "", .)
			replace `var' = subinstr(`var', `"""', "", .)
			replace `var' = subinstr(`var', `"""', "", .)
			replace `var' = subinstr(`var', ", ,", ",", .)
			replace `var' = subinstr(`var', ",,", ",", .)
			replace `var' = subinstr(`var', `"("', "", .)
			replace `var' = subinstr(`var', `")"', "", .)
			replace `var' = subinstr(`var', "()", "", .)
			replace `var' = subinstr(`var', `".."', ".", .)
			replace `var' = subinstr(`var', "´"  , "'", .)
			replace `var' = subinstr(`var', "'"  , "'", .)
			replace `var' = subinstr(`var', `"("', "", .)
			replace `var' = subinstr(`var', `")"', "", .)
			replace `var' = subinstr(`var', "()", "", .)
			replace `var' = subinstr(`var', "-", " ", .)
			replace `var' = subinstr(`var', `".."', ".", .)
			replace `var' = subinstr(`var', "´"  , "'", .)
			replace `var' = subinstr(`var', "'"  , "'", .)
			replace `var' = trim(`var')
		}
}
end

