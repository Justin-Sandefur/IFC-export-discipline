*******************************************************
* 0) Paths (edit to your local paths)
*******************************************************

if "`c(username)'"=="Justin"{
	global root 	= "/Users/Justin/Dropbox/Blog/DFI"
}
cd "$root"

global SEA_XLS  "input/WIOD/Socio_Economic_Accounts.xlsx"   	
global WIOT_DTA "input/WIOD/WIOT2014_October16_ROW.dta"			
global XWALK    "input/IFC/ifc_isic_crosswalk_divisions.csv" 	// ISIC→IFC buckets


*******************************************************
* 1) EMPLOYMENT INTENSITY (L/Y) & PRODUCTIVITY GROWTH
*    from WIOD SEA 2016 (sheet: DATA)
*******************************************************
clear all
set more off

* Import the wide SEA sheet
import excel using "$SEA_XLS", sheet("DATA") firstrow clear

* LMICs only
keep if inlist(country,"BRA","CHN","IDN","IND","MEX","RUS")

* Keep variables we need
local i = 2000
foreach var in E F G H I J K L M N O P Q R S{
	rename `var' y`i'
	local ++i
}

* Keep the core identifiers and year columns
keep country variable description code y2000-y2014

* --- Long by year (variable "value") ---
reshape long y, i(country variable description code) j(year)
destring y, g(value) force

* --- Spread indicators wide: EMP, H_EMPE, VA, VA_QI/VA_VOL become columns ---
keep country code year variable value
drop if missing(value)
reshape wide value, i(country code year) j(variable) string

* Clean names to simple variables if present
foreach v in EMP H_EMPE VA VA_QI /*VA_VOL*/ {
    capture confirm variable value`v'
    if !_rc rename value`v' `v'
}

* Need exchange rates below: putting things in USD so L/Y ratios are comparable
preserve
	clear
	input str3 country xrat
	BRA 2.35
	CHN 6.15
	IDN 11865
	IND 61.0
	MEX 13.3
	RUS 38.4
	end
	tempfile xrat
	save `xrat'
restore
mmerge country using `xrat'

* Prefer real VA if present
gen VA_real = .
destring VA_QI, replace
replace VA_real = VA_QI if !missing(VA_QI)
*replace VA_real = VA_VOL if missing(VA_QI) & !missing(VA_VOL)

replace VA 		= VA/xrat		// converting to USD
replace VA_real = VA_real/xrat	// converting to USD

* Employment intensity (persons-based; hours alternative also available)
replace EMP = EMP * 1000
gen ly_emp   = EMP   / VA   if !missing(EMP)   & !missing(VA)
gen ly_hours = H_EMPE/ VA   if !missing(H_EMPE)& !missing(VA)


* Labor productivity (prefer real VA)
gen lp_emp = .
replace lp_emp = VA_real/EMP if !missing(VA_real) & !missing(EMP)
replace lp_emp = VA/EMP      if  missing(VA_real) & !missing(VA) & !missing(EMP)

tempfile sea_panel
save `sea_panel', replace

* Collapse to ISIC division × year (code is the industry code like A01, C10, etc.)
* (We leave country dimension for now to compute global averages later)
* First compute L/Y at country-industry-year
* We already saved the panel we need in `sea_panel'

use `sea_panel', clear

* Keep the years we'll use for "static" averages (2010–2014) and the span for growth (2000–2014)
gen in_2010_14 = inrange(year, 2010, 2014)
gen in_2000_14 = inrange(year, 2000, 2014)

* For global parameters, weight by value added (current) within the averaging window
* Create weights (use VA; if missing, fallback to VA_real)
gen 	VA_w = VA
replace VA_w = VA_real if missing(VA_w)


* -- Employment intensity: average L/Y (persons-based), 2010–2014 --
preserve
	keep if in_2010_14==1
	keep country code year ly_emp VA_w
	drop if missing(ly_emp) | missing(VA_w)

	* Value-added weighted mean within country×code over 2010–2014
	bys country code: egen ly_emp_wmean_cy = total(ly_emp * VA_w)
	bys country code: egen VA_w_sum_cy     = total(VA_w)
	gen ly_emp_avg_cy = ly_emp_wmean_cy / VA_w_sum_cy

	* Keep one row per country×code
	bys country code: keep if _n==1
	keep country code ly_emp_avg_cy
	tempfile ly_c
	save `ly_c', replace
restore

* -- Productivity growth: avg. annual Δln(VA/L), 2000–2014 --
preserve
	keep if in_2000_14==1
	keep country code year VA EMP VA_real

	* Construct labor productivity (prefer real VA if present)
	gen VA_for_lp = VA_real
	replace VA_for_lp = VA if missing(VA_for_lp)
	drop if missing(VA_for_lp) | missing(EMP) | EMP<=0

	gen lprod = ln(VA_for_lp/EMP)

	* Compute annualized growth between first and last available years in 2000–2014
	bys country code (year): gen first_lprod = lprod[1]
	bys country code (year): gen first_year  = year[1]
	bys country code (year): gen last_lprod  = lprod[_N]
	bys country code (year): gen last_year   = year[_N]
	gen years_span = last_year - first_year
	drop if years_span<=0

	gen g_annual = (last_lprod - first_lprod) / years_span

	* Keep one row per country×code
	bys country code: keep if _n==_N
	keep country code g_annual
	tempfile g_c
	save `g_c', replace
restore

* Merge employment intensity and growth at country×ISIC code
use `ly_c', clear
merge 1:1 country code using `g_c', nogen

* Now compute GLOBAL (cross-country) parameters by ISIC code:
* We VA-weight across countries using the same 2010–2014 VA sums
* Build the VA weights per country×code over 2010–2014
use `sea_panel', clear
keep if inrange(year,2010,2014)
keep country code year VA VA_real
gen VA_w = VA
replace VA_w = VA_real if missing(VA_w)
collapse (sum) VA_w, by(country code)
tempfile va_c
save `va_c', replace

* Bring weights into the indicator table
use `ly_c', clear
merge 1:1 country code using `g_c', nogen
merge 1:1 country code using `va_c', nogen

* Weighted aggregation to GLOBAL per ISIC code
replace VA_w = max(VA_w,0)
collapse (mean) ly_emp_avg_cy g_annual [aw=VA_w], by(code)	/// SIMPLE OR WEIGHTED ACROSS COUNTRIES?

rename ly_emp_avg_cy ly_emp_avg_global
rename g_annual      g_lprod_annual_global

tempfile sea_isic
save `sea_isic', replace

*******************************************************
* 2) TRADABILITY from WIOT snippet (Exports / Output)
*******************************************************

use "$WIOT_DTA", clear
* Expect variables:
*   Country (3-letter), IndustryCode (e.g., C10), Year, many vXXX# columns, TOT

* LMICs only
keep if inlist(Country,"BRA","CHN","IDN","IND","MEX","RUS")

* Identify total of all v* columns (both domestic + foreign)
ds v*
local vvars `r(varlist)'

egen v_all = rowtotal(`vvars')

* Initialize domestic total
gen v_dom = .

* For each origin country, find columns whose names start with v{Country}
drop if Country=="TOT" // don't think we use this?
levelsof Country, local(clist)

foreach c of local clist {
    ds v`c'*
    local varlist `r(varlist)'
    if "`varlist'" != "" {
        egen temp = rowtotal(`varlist') if Country == "`c'"
        replace v_dom = temp if Country == "`c'"
        drop temp
    }
}

* Exports = all foreign uses (including ROW), i.e., all v* minus domestic v{Country}*
gen exports = v_all - v_dom

* Tradability ratio = exports / output
* Prefer TOT if present; else fallback to v_all as proxy
capture confirm variable TOT
gen output = .
replace output = TOT if _rc==0
replace output = v_all if missing(output)

gen tradability = exports / output
label var tradability "Exports / Output (WIOT)"

* Compute average 2010–2014 tradability at ISIC code × country, then aggregate globally
keep Country IndustryCode Year tradability output
keep if inrange(Year,2010,2014)

* Output-weighted average per country×industry
bys Country IndustryCode: egen trad_wsum = total(tradability * output)
bys Country IndustryCode: egen out_sum   = total(output)
gen trad_cy = trad_wsum / out_sum
bys Country IndustryCode: keep if _n==1

* Build global, output-weighted average per ISIC code
collapse (mean) trad_cy [aw=out_sum], by(IndustryCode) /// SIMPLE OR WEIGHTED ACROSS COUNTRIES?

rename IndustryCode code
rename trad_cy tradability_global

tempfile wiot_isic
save `wiot_isic', replace

*******************************************************
* 3) Merge the three indicators at ISIC code level
*******************************************************
use `sea_isic', clear
merge 1:1 code using `wiot_isic', nogen

* Optional: keep a clean panel for your 3 metrics at ISIC code level
order code ly_emp_avg_global g_lprod_annual_global tradability_global
tempfile metrics_isic
save `metrics_isic', replace


*******************************************************
* 4) Map ISIC codes → IFC buckets and aggregate (VA-weighted)
*******************************************************
* Load crosswalk (must contain: "ISIC Rev.4 division" (e.g., C10) and "IFC industry bucket")
import delimited using "$XWALK", clear varnames(1) stringcols(_all)
rename (isicrev4divis ifcindustryb) (code ifc_bucket)
tempfile crosswalk
save `crosswalk'

* Merge with metrics
merge 1:m code using `metrics_isic', keep(match) nogen

* For aggregation weights, use global VA weights from SEA (2010–2014)
* Bring in VA weights per code (already computed earlier)
use `sea_panel', clear
keep if inrange(year,2010,2014)
keep code VA VA_real
gen VA_w = VA
replace VA_w = VA_real if missing(VA_w)
collapse (sum) VA_w, by(code)
tempfile va_isic
save `va_isic', replace


use `metrics_isic', clear
merge 1:1 code using `va_isic', nogen
merge m:1 code using `crosswalk', keep(match) nogen


* Aggregate to IFC buckets (value-added weighted means)
collapse (mean) ly_emp_avg_global g_lprod_annual_global tradability_global [aw=VA_w], by(ifc_bucket)

* Format & label
label var ly_emp_avg_global       "Employment intensity L/VA (persons-based), 2010–2014 avg"
label var g_lprod_annual_global   "Annualized growth of labor productivity (VA/L), 2000–2014"
label var tradability_global      "Exports/Output, 2010–2014 avg"

sort ifc_bucket
list, noobs abbrev(24)

* Index
sort ly
gen rank_emp = _n
sort g_lprod
gen rank_prod = _n
sort trad
gen rank_trad = _n
*gen index = rank_emp + rank_prod + rank_trad
gen index = ly*g*trad
gsort -index
keep ifc_bucket index ly_emp_avg_global   g_lprod_annual_global   tradability_global 
list ifc_bucket index ly_emp_avg_global   g_lprod_annual_global   tradability_global , noobs abbrev(24)

* Save final 9×3 table
outsheet using output/ifc_sector_parameters.csv, comma replace

exit
