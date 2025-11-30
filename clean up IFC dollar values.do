/*
This program starts with the raw IFC project-level data
and combines it with data scraped from the IFC website.

The motivation is that the project-level database contains
dollar values that appear wildly inflated in some cases.
For example, this is listed in the database as a $1 billion 
guarantee, but the web entry suggests it's worth $10 million!
https://disclosures.ifc.org/project-detail/SII/44597/gtfp-unico-mozam

Basic approach is to use the largest number on the web if
it is smaller than the number in the database. 
And secondly, to flag trade finance programs which seem to
be excluded from IFC long-term investment totals in reports.
*/

*** Scraped data
	forval n = 1/2{
		insheet using "output/out_ifc_sections_api_`n'.csv", clear
		rename ifc_investment_note web_note
		rename ifc_investment_usd web_usd
		rename all_amount_mentions web_amount_mentions
		rename facility_notional_usd web_usd2
		rename facility_note web_note2
		rename section_text web_text
		rename project_id projectnumber
		keep projectnumber fetch_status web*
		tempfile scraped`n'
		save `scraped`n''
	}
	append using `scraped1'
	drop if web_usd==. & web_usd2==.
	replace web_usd  = web_usd  / 1e6
	replace web_usd2 = web_usd2 / 1e6
	duplicates drop projectnumber, force
	save `scraped2', replace

*** Downloaded database
	insheet using "input/IFC/ifc_investment_services_projects_11-05-2025.csv", clear
	mmerge projectnumber using `scraped2'
	
*** Dates
	split datedisclosed, g(date) p("/")
	destring date3, g(year)

*** Rule: use greatest of scraped values. Then use lesser of scraped vs downloaded value.
	gen web_y = max(web_usd,web_usd2)
	gen y = min(total,web_y)
		
*** Manual
	replace y = 10 if projectnumber==48853	// unclear how web scraper got $31 billion!! Real value is ~$10m (listed in MAD)
	replace y = 12 if projectnumber==43468	// swamps LIC totals if not fixed: web says max of 10m Euros; dataset lists $200m
	replace y = 17 if projectnumber==44974	// swamps LIC totals if not fixed: web says max of 15m Euros; dataset lists $200m
	replace y = totalifc if inlist(projectnumber,33800,37649,43239) // unclear why it changed this
	
*** Flag trade finance
	gen tradefinance = 0
	
	foreach p in "GSCF" "GTLP" "GTSF" "GTFP" "Global Trade Liquidity Program" "Liquidity Program" "Global Trade Supplier Finance" "Trade Finance" "Trade finance" "Supplier Finance" "Supplier finance"{
		replace tradefinance = 1 if regexm(projectname,"`p'")
	}
	replace tradefinance = 1 if projectnumber==34934
	
	gen ynotrad = y if tradefinance==0
	gen totalnotrad = total if tradefinance==0

*** Weird cases where web has a number but IFC claims none. 
	replace y = . if totalifc==.
	replace ynotrad = . if totalifc==.
	
	
***************
*** EXPORTS ***
***************
	
	preserve
		insheet using output/out_ifc_exports2.csv, clear
		tempfile x
		save `x'
	restore
	mmerge projectnumber using `x', umatch(project_id)

*** Some manual cleaning
	replace export_hits = 0 if projectnumber==43582
	replace export_hits = 0 if projectnumber==34623
	
save output/ifc_clean.dta, replace


*** NB: this really starts to bit in the $100s of millions, and a cluster at $1 billion
	gen ly = log10(y)
	gen ltotal = log10(totalifc)
	#delimit ;
		tw 	(scatter ly ltotal if tradefinance==0, mcolor(blue))
			(scatter ly ltotal if tradefinance==1, mcolor(red)), 
			xtitle("Raw numbers") ytitle("Cleaned numbers") 
			legend(order(2 "Trade finance" 1 "Other"))
			xlabel(-1 "$100k" 0 "$1m" 1 "$10m" 2 "$100m" 3 "$1B" 4 "$10B")
			ylabel(-1 "$100k" 0 "$1m" 1 "$10m" 2 "$100m" 3 "$1B" 4 "$10B")
		;
	#delimit cr


*** Check against IFC's published totals: https://www.ifc.org/content/dam/ifc/doc/2024/ifc-annual-report-2024-year-in-review.pdf?utm_source=chatgpt.com
	*gsort -total
	*bro total y ynotr projectname if year==2024
	collapse (sum) y ynotrad total*, by(industry year)
	format y ynotrad total* %20.0fc
	list if year==2024
	
exit
