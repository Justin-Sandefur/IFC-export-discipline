
******************
*** FILE PATHS ***
******************

	if "`c(username)'"=="Justin"{
		global root 	= "/Users/Justin/Dropbox/Blog/DFI"
	}
	cd "$root"
		
************************
*** WDI country info ***
************************
/*
	insheet using input/ISOcrosswalk.csv, clear
	tempfile iso
	save `iso'

	wbopendata, indicator(BX.KLT.DINV.CD.WD;NY.GDP.MKTP.CD;NY.GDP.PCAP.PP.CD;SP.POP.TOTL) clear long
	rename bx_klt fdi_usd
	rename ny_gdp_mktp gdp_usd
	rename ny_gdp_pcap_pp gdp_pcppp
	rename sp_pop pop
	mmerge countrycode using `iso', unmatched(none)
	drop _merge iso*
	
	save output/wdi.dta, replace
*/
	

*********************
*** SECTOR LABELS ***
*********************

*** Rank sectors by a somewhat arbitrary index of growthiness: data from OECD via chatGPT
	clear
	input str48 industry Prod_growth Tradability Linkages Knowledge_intensity Employment_intensity
	"Manufacturing"								5	5	5	4	3
	"Telecommunications & Technology"			4	4	3	5	2
	"Health, Education & Life Sciences"			3	2	3	4	4
	"Other"										3	3	3	2	3
	"Metals & Mining"							3	4	2	2	1
	"Agribusiness & Forestry"					2	3	3	2	4
	"Infrastructure"							2	1	4	2	4
	"Tourism, Retail & Property"				2	4	2	1	5
	"Financial Markets"							3	1	2	3	1
	"Funds"										3	1	2	2	1
	end
	gen score = Trad + Emp + Prod + Linkages	// core of the ranking
	gsort -score
	gen irank = _n
	keep industry irank
	save output/sectors.dta, replace
	
*** Newer alternative based on WIOD data, see separate do file
	local rankvar = "ly"
	insheet using output/ifc_sector_parameters.csv, clear
	rename ifc_bucket industry
	replace industry = "Telecomms & tech" if industry=="Telecommunications & Technology"
	replace industry = proper(industry)
	set obs 10
	replace industry="Funds" if industry==""
	
*drop if inlist(industry,"Other","Funds","Financial Markets")

	*** Loop over dimensions
		foreach rankvar in ly g_ trad{
			replace `rankvar' = . if industry=="Other"
			gsort -`rankvar' 
			gen rank_`rankvar' = _n
			replace rank_`rankvar' = rank_`rankvar'+2 if rank_`rankvar'>4
			replace rank_`rankvar' = 5 if industry=="Other"
			replace rank_`rankvar' = 6 if industry=="Funds"
			labmask rank_`rankvar', values(industry)
		}
		
	recast double ly g_ trad
	keep industry ly g_ trad rank_*
	save output/sectors.dta, replace

*** Saving industry codes for later
	foreach rankvar in ly g_ trad{
		local vl : value label rank_`rankvar'
		levelsof rank_`rankvar', local(codes_`rankvar')
		local names_`rankvar'
		foreach c of local codes_`rankvar' {
			local nm_`rankvar' : label `vl' `c'
			local names_`rankvar' `"`names_`rankvar'' "`nm_`rankvar''"'"'
			local i`rankvar'_`c' `"`nm_`rankvar''"'   // e.g., `i_3' is the name for code 3
		}
	}
	
	keep if ly!=.
	drop rank*
	outsheet using output/sectors.csv, comma replace		// for 1st heatmap graph
	
	
******************************
*** IFC project-level data ***
******************************

	*insheet using "input/IFC/ifc_investment_services_projects_11-05-2025.csv", clear
	use output/ifc_clean.dta, clear
	replace wbcountrycode="CD" if wbcountrycode=="ZR"
	replace industry = "Telecomms & tech" if industry=="Telecommunications & Technology"
	
*** Bring in FDI, GDP
	drop y totalnotrad totalifc 	// choose outcome metric, various cleaning options
	rename ynotrad y
	collapse (sum) y, by(country wbcountrycode industry year)
	
*** Make sure all country-industry cells exist before taking averages
	preserve
		duplicates drop industry, force
		keep industry
		drop if industry==""
		tempfile i
		save `i'
	restore
	preserve
		append using output/wdi.dta
		duplicates drop wbcountrycode, force
		keep wbcountrycode
		drop if wbcountrycode==""
		cross using `i'
		tempfile ci
		save `ci'
	restore
	preserve
		duplicates drop year, force
		keep year
		drop if year==.
		cross using `ci'
		tempfile ciy
		save `ciy'
	restore
	mmerge wbcountrycode industry year using `ciy'
	drop if _merge==-1	// regional aggregates mostly

	mmerge wbcountrycode year using output/wdi.dta
	*keep if year>=2020 & year<=2024 

	collapse (mean) y fdi_usd gdp_* pop, by(wbcountrycode industry lendingtype incomelevel year) // cut year
	
*** Save data, collapse differently for different graph types
	replace industry = proper(industry)
	mmerge industry using output/sectors.dta
	drop _merge 
	save output/ifc_country_sector.dta, replace

************************
*** GRAPH: Exporting ***
************************

*** Back to raw-er data, project level
	use output/ifc_clean.dta, clear
	replace wbcountrycode="CD" if wbcountrycode=="ZR"
	replace wbcountrycode="YE" if wbcountrycode=="RY"
	mmerge wbcountrycode year using output/wdi.dta
	bysort wbcountrycode: egen mincome = mode(incomelevel)
	bysort wbcountrycode: egen mlend = mode(lendingtypename)
	replace incomelevel=mincome
	replace lendingtypename=mlend
	drop mincome
	
	keep if year>1995
	drop if incomelevel==""
	tempfile freezehere
	save `freezehere'
	
	replace incomelevel="UMC" if incomelevel=="HIC"
	replace ynotrad = ynotrad/1000
	gen y0 = ynotrad if industry!="Manufacturing" 
	gen y1 = ynotrad if industry=="Manufacturing" & (export_hits==0 | export_hits==.)
	gen y2 = ynotrad if industry=="Manufacturing" & (export_hits >0 | export_hits!=.)

	collapse (sum) y0 y1 y2, by(/*incomelevel*/ year)
	
	graph bar y2 y1 y0, over(year) /*over(incomelevel)*/ stack
	
	order year /*incomelevel*/ y2 y1 y0
	/*
	replace incomelevel="Low-income" if incomelevel=="LIC"
	replace incomelevel="Lower-middle income" if incomelevel=="LMC"
	replace incomelevel="Upper-middle & high income" if incomelevel=="UMC"
	*/
	format y0 y1 y2 %5.3fc
	outsheet using output/exports.csv, comma replace

exit
*** By IDA status
	use `freezehere', clear
	*keep if industry=="Manufacturing"
	*keep if export_hits!=.
	
	gen y0 = ynotrad 
	gen y1 = ynotrad if lendingtypename=="IDA"
	gen y2 = ynotrad if lendingtypename=="IDA" & industry=="Manufacturing"
	gen y3 = ynotrad if lendingtypename=="IDA" & industry=="Manufacturing" & export_hits>0 & export_hits!=.
	
	*** Switch to stackable, mutually exclusive categories
		replace y0 = min(y0,y0 - y1)
		replace y1 = min(y1,y1 - y2)
		replace y2 = min(y2,y2 - y3)
		
	collapse (sum) y0 y1 y2 y3, by(year)
	
	forval n = 0/3{
		*gen ly`n' = log10(y`n')
	}
	
	#delimit ;
	tw 	(line y0 year)
		(line y1 year)
		(line y2 year)
		(line y3 year),
		legend(off)
		;
	#delimit cr
	graph bar y3 y2 y1 y0, over(year) stack
	
	sort year
	format y* %10.0fc
	order year y3 y2 y1 y0
	outsheet using output/exports.csv, comma replace

	
*********************************
*** GRAPH: Simple composition ***
*********************************
/*
	use output/ifc_country_sector.dta, replace
	keep if year>2019 & year<2025
	drop if incomelevel==""		// mostly aggregates
	collapse (sum) y (mean) ly, by(incomelevel industry)
	reshape wide y, i(industry) j(incomelevel) string
	gsort -ly
	format y* %10.0fc
	order industry yLIC yLMC yUMC yHIC
	list

	outsheet industry yLIC yLMC yUMC yHIC using output/industry_heatmap.csv, comma replace


*** Just FYI, what's driving the decline: IDA
	use output/ifc_country_sector.dta, replace
	keep if year>1995
	rename y y_
	gen ida = lendingtype=="IDX"
	collapse (sum) y_, by(year ida)
	reshape wide y_, i(year) j(ida) 
	egen total = rsum(y_*)
	foreach var of varlist y_*{
		gen share`var' = 100*`var'/total
	}
	order 
	graph bar  sharey_1 sharey_0, over(year, gap(0) label(angle(45))) stack legend(order(1 "IDA" 2 "Non-IDA"))
	

*** Just FYI, what's driving the decline: income groups
	use output/ifc_country_sector.dta, replace
	keep if year>1995
	rename y y_
	collapse (sum) y_, by(year incomelevel)
	drop if incom==""
	reshape wide y_, i(year) j(income) string
	egen total = rsum(y_*)
	foreach var of varlist y_*{
		gen share`var' = 100*`var'/total
	}
	order *LIC *LMC *UMC *HIC
	graph bar sharey_*, over(year, gap(0) label(angle(45))) stack legend(order(4 "HIC" 3 "UMC" 2 "LMC" 1 "LIC"))


*** Just FYI, what's driving the decline: industries
	use output/ifc_country_sector.dta, replace
	rename y y_
	collapse (sum) y_, by(year industry)
	drop if industr==""
	replace industry = substr(industry,1,4)
	reshape wide y_, i(year) j(industry) string
	egen total = rsum(y_*)
	foreach var of varlist y_*{
		gen share`var' = 100*`var'/total
	}
	graph bar sharey_*, over(year) stack
*/

*******************************************
*** GRAPH: Overall labor intensity, etc ***
*******************************************
	
*** By income
	use output/ifc_country_sector.dta, replace
	keep if year>2019

	*** Weighting the values by volume but not the GDP variable; probably doesn't matter, just confusing
		preserve
			collapse (mean) ly g_ trad [aw=y], by(wbc)
			tempfile values
			save `values'
		restore
		collapse (mean) gdp_pcppp [aw=y], by(wbc) 
		mmerge wbc using `values'

	gen loggdp = log10(gdp_pcppp)
	
	reg ly loggdp
	reg g_ loggdp
	reg trad loggdp
	
	tw (lpolyci ly loggdp)(sc ly loggdp), legend(off)
	
	*** Create a grid for the smoothing results
		gen x = _n/10
		
	foreach var in ly g_ trad{
		lpoly `var' loggdp, nograph ci at(x) gen(value`var') se(se`var')
		gen lo`var' = value`var'-1.96*se`var'
		gen hi`var' = value`var'+1.96*se`var'
	}
	
	keep if x>=3 & x<=5
	keep x value* lo* hi*
	drop loggdp
	reshape long value lo hi, i(x) j(variable) string
	replace variable = "Labor intensity (L/Y)" if variable=="ly"
	replace variable = "Growth in value added" if variable=="g_"
	replace variable = "Export propensity (X/Y)" if variable=="trad"
	
	outsheet using output/by_gdp.csv, comma replace
	

*** Over time
	use output/ifc_country_sector.dta, replace
	keep if year>1995

drop if industry=="Financial Markets"

	collapse (mean) ly g_ trad [aw=y], by( year) // incomelevel instead
	format ly g_ trad %5.2fc
	list
	
	tw (lpolyci trad year)(sc trad year), legend(off)
	

	foreach var in ly g_ trad{
		lpoly `var' year, nograph ci at(year) gen(value`var') se(se`var')
		gen lo`var' = value`var'-1.96*se`var'
		gen hi`var' = value`var'+1.96*se`var'
	}
	
	keep year value* lo* hi*
	reshape long value lo hi, i(year) j(variable) string
	replace variable = "Labor intensity (L/Y)" if variable=="ly"
	replace variable = "Growth in value added" if variable=="g_"
	replace variable = "Export propensity (X/Y)" if variable=="trad"
	
	outsheet using output/trends.csv, comma replace
	
exit

*****************************************
*** GRAPH: Stacked bar by sector rank ***
*****************************************

	use output/ifc_country_sector.dta, clear

	*** Annoyingly precision limits are creating fake variance within countries
		foreach var of varlist fdi gdp* pop ly g_ trad{
			bysort wbcountry: egen m`var' = mean(`var')
			replace `var' = m`var'
			drop m`var'
		}
	
*** Wide with industry rankings
	drop industry
	tempfile a b
	preserve
		drop rank_ly rank_g_
		reshape wide y, i(wbcountry) j(rank_trad)
		rename y* y*_trad
		save `a'
	restore
	preserve
		drop rank_ly rank_trad
		reshape wide y, i(wbcountry) j(rank_g_)
		rename y* y*_g_
		save `b'
	restore
	drop rank_g_ rank_trad 
	reshape wide y, i(wbcountry) j(rank_ly)
	rename y* y*_ly
	mmerge wbc using `a'
	mmerge wbc using `b'



****************************
*** VARIOUS DENOMINATORS ***
****************************

*** Relative to GDP, etc
	egen ytotal = rsum(y*_ly)
	replace fdi_usd = max(1e7,fdi_usd)			// negative FDI flows are a weird denominator: imposting $10m as floor, about 5th percentile of non-zero countries
	gen y_fdi = 100*1e6*ytotal/fdi_usd
	gen y_gdp = 100*1e6*ytotal/gdp_usd
	
*** Relative to IFC global portfolio
	sum ytotal
	local total = r(sum)
	gen y_ifc = 100*ytotal/`total'

*** Industry shares
	foreach rankvar in ly trad g_{
		forval y=1/10{
			capture replace y`y'_`rankvar'=0 if y`y'_`rankvar'==.
			capture gen y`y'share_`rankvar'=100*y`y'_`rankvar'/ytotal
		}
	}

*** Regressions on GDP per capita
	*replace ytotal_fdi = 100 if ytotal_fdi>100
	gen lgdp_usd = ln(gdp_usd)
	gen lgdp_pcppp = ln(gdp_pcppp)
	gen lpop = ln(pop)
	reg y_ifc lgdp_pcppp pop // lgdp_usd
	reg y_fdi lgdp_pcppp pop // lgdp_usd
	foreach var in y_ifc y_fdi y_gdp lgdp_pcppp{
		reg `var' lpop lgdp_usd 
		predict `var'_r, r
	}
	*tw (lfitci y_gdp_r lgdp_pcppp_r)(sc y_gdp_r lgdp_pcppp_r)
	

*** Country sample
	drop if wbcountrycode==""
	mmerge wbcountrycode using output/wdi.dta, uif(year==2023) ukeep(country countrycode regionname income*) unmatched(none)
	replace countrycode="COD" if countrycode=="ZAI"
	*keep if inlist(countrycode,"NGA","ETH","MOZ","PAK","CHN","MEX","EGY","VNM") | inlist(countrycode,"IND","KEN","CIV","TUR","ZAF","IRQ","COL","ROM")


*** Income groupings
	collapse (mean) y* lgdp_pcppp [aw=ytotal], by(incomelevel)
	
*** Jobs
	local unit = "incomelevel"	// country
	local rankvar = "ly"
	#delimit ;
	graph hbar y1_`rankvar'-y10_`rankvar', 
		over(`unit', sort(lgdp_pcppp)) stack
		bar(1, color(navy))
		bar(2, color(navy%90))
		bar(3, color(navy%80))
		bar(4, color(navy%70))
		bar(5, color(navy%60))
		bar(6, color(navy%50))
		bar(7, color(navy%40))
		bar(8, color(navy%30))
		bar(9, color(navy%20))
		bar(10, color(navy%10))
		ytitle("Share of IFC portfolio within each country", size(small))
		title("{bf:Fewer jobs for poorer countries}""Share of IFC portfolio by industry, ranked by labor intensity", justification(left) astextbox span margin(b+2))
		legend(order(1 "`i`rankvar'_1'" 2 "`i`rankvar'_2'" 3 "`i`rankvar'_3'" 4 "`i`rankvar'_4'" 5 "`i`rankvar'_5'" 6 "`i`rankvar'_6'" 7 "`i`rankvar'_7'" 8 "`i`rankvar'_8'" 9 "`i`rankvar'_9'" 10 "`i`rankvar'_10'") pos(10))
		ysc(noline) ylabel(,notick) 
		subtitle("{it:Industries:}                                                             {it:Countries:}""From most to least labor intensive                        From poorest to richest", justification(left) astextbox span) 
		note("IFC investments include equity, lending, and guarantees, 2020-24. Labor intensity is based on data from the World""Input-Output Database for available LMICs (Brazil, Russia, India, China, Indonesia, and Mexico). IFC investments""in pooled 'funds' and 'other' are assigned the median values for labor intensity.", span color(gs8))
		plotregion(margin(b-2))
		ylabel(0 "0%" 20 "20%" 40 "40%" 60 "60%" 80 "80%" 100 "100%")
		//ysize(6) scale(.75)
		;
	#delimit cr
	gr_edit grpaxis.style.editstyle linestyle(width(none)) editcopy

*** Trade
	local rankvar = "trad"
	#delimit ;
	graph hbar y1share_`rankvar'-y10share_`rankvar', 
		over(`unit', sort(lgdp_pcppp)) stack
		bar(1, color(maroon))
		bar(2, color(maroon%90))
		bar(3, color(maroon%80))
		bar(4, color(maroon%70))
		bar(5, color(maroon%60))
		bar(6, color(maroon%50))
		bar(7, color(maroon%40))
		bar(8, color(maroon%30))
		bar(9, color(maroon%20))
		bar(10, color(maroon%10))
		ytitle("Share of IFC portfolio within each country", size(small))
		title("{bf:Fewer exports for poorer countries}""Share of IFC portfolio by industry, ranked by trade/value-added", justification(left) astextbox span margin(b+2))
		legend(order(1 "`i`rankvar'_1'" 2 "`i`rankvar'_2'" 3 "`i`rankvar'_3'" 4 "`i`rankvar'_4'" 5 "`i`rankvar'_5'" 6 "`i`rankvar'_6'" 7 "`i`rankvar'_7'" 8 "`i`rankvar'_8'" 9 "`i`rankvar'_9'" 10 "`i`rankvar'_10'") pos(10))
		ysc(noline) ylabel(,notick) 
		subtitle("Industries:                                                             Countries:""From most to least tradable                        From poorest to richest", justification(left) astextbox span) 
		//note("IFC investments include equity, lending, and guarantees, 2020-24. Tradability measures exports over output based on data from""the World Input-Output Database for available LMICs (Brazil, Russia, India, China, Indonesia, and Mexico).""IFC investments in pooled 'funds' and 'other' are assigned the median values.", span color(gs8))
		;
	#delimit cr
	gr_edit grpaxis.style.editstyle linestyle(width(none)) editcopy


	local rankvar = "g_"
	#delimit ;
	graph hbar y1share_`rankvar'-y10share_`rankvar', 
		over(`unit', sort(lgdp_pcppp)) stack
		bar(1, color(green))
		bar(2, color(green%90))
		bar(3, color(green%80))
		bar(4, color(green%70))
		bar(5, color(green%60))
		bar(6, color(green%50))
		bar(7, color(green%40))
		bar(8, color(green%30))
		bar(9, color(green%20))
		bar(10, color(green%10))
		title("{bf:Less growth for poorer countries}""Industry shares of IFC commitments (equity, lending, & guarantees)""in its 15 largest country programs, 2020-24", justification(left) astextbox span margin(b+2))
		legend(order(1 "`i`rankvar'_1'" 2 "`i`rankvar'_2'" 3 "`i`rankvar'_3'" 4 "`i`rankvar'_4'" 5 "`i`rankvar'_5'" 6 "`i`rankvar'_6'" 7 "`i`rankvar'_7'" 8 "`i`rankvar'_8'" 9 "`i`rankvar'_9'" 10 "`i`rankvar'_10'") pos(10))
		ysc(noline) ylabel(,notick) 
		subtitle("Industries:                                                             Countries:""From fastest to slowest growing                        From poorest to richest", justification(left) astextbox span) 
		note("IFC investments include equity, lending, and guarantees, 2020-24. Productivity growth is based on data from the World""Input-Output Database for available LMICs (Brazil, Russia, India, China, Indonesia, and Mexico) for 2000 to 2014.""IFC investments in pooled 'funds' and 'other' are assigned the median values.", span color(gs8))
		;
	#delimit cr
	gr_edit grpaxis.style.editstyle linestyle(width(none)) editcopy

exit

*** Add scale to legend
	gr_edit AddTextBox added_text editor 8.39997716887604 99.2155924050353
	gr_edit added_text_new = 4
	gr_edit added_text_rec = 3
	gr_edit added_text[3].style.editstyle  angle(default) size( sztype(relative) val(3.4722) allow_pct(1)) color(black) horizontal(left) vertical(middle) margin( gleft( sztype(relative) val(0) allow_pct(1)) gright( sztype(relative) val(0) allow_pct(1)) gtop( sztype(relative) val(0) allow_pct(1)) gbottom( sztype(relative) val(0) allow_pct(1))) linegap( sztype(relative) val(0) allow_pct(1)) drawbox(no) 
	gr_edit added_text[3].style.editstyle size(small) editcopy
	gr_edit added_text[3].text = {}
	gr_edit added_text[3].text.Arrpush Less labor intensive

exit

*** y extents (number of categories)
	levelsof countrycode, local(cats)
	local ymin 0.5
	local ymax = wordcount("`cats'") + 0.5

*** add a vertical double-headed arrow just left of x=0
	gr_edit plotregion1.add_arrow arrow1, ///
    x1(-2) y1(0.5)  x2(-2) y2(8)  ///
    head(both) lcolor(black) lwidth(medthick)
exit

*** end labels (text boxes) near the arrow tips
	gr_edit plotregion1.add_text x -2 y `ymax' text "Richer countries", place(w) size(small)
	gr_edit plotregion1.add_text x -2 y `ymin' text "Poorer countries", place(w) size(small)

*** widen left margin if needed so nothing is clipped
	graph display G, xsize(8) ysize(6) plotregion(margin(large)) xscale(range(-5 100))

exit	
	order country countrycode wbcountrycode ytotal_fdi ytotal_ifc 
	keep country countrycode wbcountrycode y*
	outsheet using output/map.csv, comma replace
	
exit

	drop year
	graph hbar y*share, over(country, sort(y1_share)) stack legend(off) ysize(12)
	
exit
