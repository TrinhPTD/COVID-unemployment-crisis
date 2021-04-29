libname sasproj 'm:/seane/SASProject';

data pulse;
set sasproj.pulse2020_PUF_01 sasproj.pulse2020_puf_02 sasproj.pulse2020_puf_03 sasproj.pulse2020_puf_04
sasproj.pulse2020_puf_05 sasproj.pulse2020_puf_06 sasproj.pulse2020_puf_07 sasproj.pulse2020_puf_08 sasproj.pulse2020_puf_09
sasproj.pulse2020_puf_10 sasproj.pulse2020_puf_11 sasproj.pulse2020_puf_12;
run;

filename oxford url "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv";
proc import file=oxford out=work.oxford dbms=csv replace;
guessingrows=max;
run;

data oxford_datemodify;
set work.oxford;
newdate = input(put(date,8.),yymmdd8.);
format newdate date10.;
drop date;
run;

proc import datafile = "m:\seane\SASProject\Region.csv"
out = work.region replace
dbms=csv;
run;

data work.oxford_prelim;
set oxford_datemodify;
format week comma15.;
   if   '23APR2020'd<= newdate <='5MAY2020'd  then week='1';
   if   '7MAY2020'd<= newdate <='12MAY2020'd  then week='2';
   if   '14MAY2020'd<= newdate <='19MAY2020'd  then week='3';
   if   '21MAY2020'd<= newdate <='26MAY2020'd  then week='4';
   if   '28MAY2020'd<= newdate <='2JUN2020'd  then week='5';
   if   '4JUN2020'd<= newdate <='9JUN2020'd  then week='6';
   if   '11JUN2020'd<= newdate <='16JUN2020'd  then week='7';
   if   '18JUN2020'd<= newdate <='23JUN2020'd  then week='8';
   if   '25JUN2020'd<= newdate <='30JUN2020'd  then week='9';
   if   '2JUL2020'd<= newdate <='7JUL2020'd  then week='10';
   if   '9JUL2020'd<= newdate <='14JUL2020'd  then week='11';
   if   '16JUL2020'd<= newdate <='21JUL2020'd  then week='12';
   
      if week in ('1','2','3', '4', '5', '6', '7', '8', '9', '10', '11', '12');
      if CountryCode='USA';
      if not missing(RegionCode);
   run;
 
proc sql;
create table work.oxford_final as
select week ,  substr(RegionCode,4,2) as state, 
ROUND(AVG("GovernmentResponseIndex"n),2) as govt_response_index,
ROUND(AVG("ContainmentHealthIndex"n),2) as containment_health_index,
ROUND(AVG("EconomicSupportIndex"n),2) as economic_support_index,
ROUND(AVG("StringencyIndex"n),2) as stringency_index,
ROUND(AVG("StringencyLegacyIndex"n),2) as stringency_legacy_index
from work.oxford_prelim
group by week,RegionCode;
run;
   
 

data pulse_withmsa;
set pulse(where=(not missing(Est_MSA)));
run;

filename fips url 
'https://www2.census.gov/geo/docs/reference/codes/files/national_county.txt';
data fips replace;
infile fips dsd;
   input stnm:$char2. stcd:$char2. countycd:$char3. county:$char35. area:$char2.; 
run;

data population;
 infile  "M:\seane\SASProject\co-est2019-alldata.csv" dlm=',' firstobs=2;
 input dummy dummy dummy STATE:$char2. COUNTY:$char3. dummy dummy dummy dummy dummy dummy dummy dummy dummy dummy dummy dummy dummy POPESTIMATE2019:comma10.;
 fips=cats(STATE,COUNTY);
 drop dummy;
run;





filename covid url 
"https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv";

data covid replace;
infile covid dsd firstobs=2;
   input date:yymmdd10. county:$char30. state:$char10. fips:$char5. cases:comma15. deaths:comma15. week:comma15.;
    Format date yymmdd10. county char30. state char10. fips char5. cases comma15. deaths comma15. week comma15. ;
   if   '23APR2020'd<= date <='5MAY2020'd  then week='1';
   if   '7MAY2020'd<= date <='12MAY2020'd  then week='2';
   if   '14MAY2020'd<= date <='19MAY2020'd  then week='3';
   if   '21MAY2020'd<= date <='26MAY2020'd  then week='4';
   if   '28MAY2020'd<= date <='2JUN2020'd  then week='5';
   if   '4JUN2020'd<= date <='9JUN2020'd  then week='6';
   if   '11JUN2020'd<= date <='16JUN2020'd  then week='7';
   if   '18JUN2020'd<= date <='23JUN2020'd  then week='8';
   if   '25JUN2020'd<= date <='30JUN2020'd  then week='9';
   if   '2JUL2020'd<= date <='7JUL2020'd  then week='10';
   if   '9JUL2020'd<= date <='14JUL2020'd  then week='11';
   if   '16JUL2020'd<= date <='21JUL2020'd  then week='12';

run;

proc sort data=covid out=covid_sort;
by state county date;
run;

data covid_daily;
set covid_sort;
format LAG_CASES comma15. daily_total comma15.;
by  STATE COUNTY DATE; 
LAG_CASES = LAG(CASES);     
IF FIRST.COUNTY THEN DO;     
LAG_CASES = .; 
END;
IF LAG_CASES =. then daily_total= CASES;
IF LAG_CASES<>. then daily_total= CASES-LAG_CASES;
DROP LAG_CASES;
if week in ('1','2','3', '4', '5', '6', '7', '8', '9', '10', '11', '12');
run;


libname reffile XLSX "M:/seane/SASProject/metrofips.xlsx";
data metrofips;
set reffile.list1;
fips = cats(fipsst, fipscty);
keep fips cbsa CBSATitle;
run;

proc sql;
create table covidtotals as
Select 	week, est_msa, e.CBSATitle, totcases, CBSA_Population, totcases/CBSA_POPULATION * 100000  as cbsa_infectionrate FORMAT=comma15. 
from 
(select week FORMAT=char10.,cbsa as est_msa,CBSATitle, SUM(daily_total) as totcases 
from covid_daily a
INNER JOIN metrofips c on a.fips = c.fips
group by week,cbsa, CBSATitle)e
INNER JOIN 
(Select cbsa, SUM(POPESTIMATE2019) as CBSA_POPULATION
FROM Population b
INNER JOIN metrofips c on b.fips = c.fips
group by cbsa)d
on e.est_msa=d.cbsa;
run;



proc sort data=pulse_withmsa out=pulse_withmsa_sort;
by week est_msa;
run;

data covid_pulse;
merge pulse_withmsa_sort (in=p) covidtotals (in=c);
if p;
by week est_msa;
run;

data industry_transpose_ready;
set sasproj.industry;
keep est_msa industry february_employment average_weekly_wage;
run;

proc sort data=industry_transpose_ready out =industry_sort;
by est_msa;
where est_msa is not null;
run;


proc transpose data=industry_sort (drop=average_weekly_wage)
out=transpose_employ(drop= _name_ _label_)
prefix=em_;
var february_employment;
id industry;
by est_msa;
run;

proc transpose data=industry_sort (drop=february_employment)
out=transpose_wage(drop= _name_ _label_)
prefix=wg_;
var average_weekly_wage;
id industry;
by est_msa;
run;

data merge_industry ;
merge transpose_wage transpose_employ;
rename 'wg_102 Service-providing'n=wg_service 'wg_1022 Information'n=wg_information 'wg_1029 Unclassified'n=wg_unclassified
'wg_1013 Manufacturing'n =wg_manufacturing 'wg_1011 Natural resources and mi'n=wg_naturalresources
'wg_1027 Other services'n=wg_otherservices 'wg_1012 Construction'n=wg_construction 'wg_1026 Leisure and hospitality'n=wg_leisure
 'wg_1023 Financial activities'n=wg_finance 'wg_1025 Education and health ser'n=wg_health_education 'wg_1024 Professional and busines'n=wg_professional
 'wg_101 Goods-producing'n=wg_goods 'wg_1021 Trade, transportation, a'n=wg_trade
'em_102 Service-providing'n=emp_service 'em_1022 Information'n=emp_information 'em_1029 Unclassified'n=emp_unclassified
'em_1013 Manufacturing'n =emp_manufacturing 'em_1011 Natural resources and mi'n=emp_naturalresources
'em_1027 Other services'n=emp_otherservices 'em_1012 Construction'n=emp_construction 'em_1026 Leisure and hospitality'n=emp_leisure
 'em_1023 Financial activities'n=emp_finance 'em_1025 Education and health ser'n=emp_health_education 'em_1024 Professional and busines'n=emp_professional
 'em_101 Goods-producing'n=emp_goods 'em_1021 Trade, transportation, a'n=emp_trade;
by est_msa;
avg_wg=MEAN('wg_102 Service-providing'n - - 'wg_1029 Unclassified'n);
total_emp=SUM(OF 'em_102 Service-providing'n - - 'em_1029 Unclassified'n);

run;

data industry_calculations;
set merge_industry;
format service_emp_pct comma15. information_emp_pct comma15. trade_emp_pct comma15.
unclassified_emp_pct comma15. manufacturing_emp_pct comma15. naturalresources_emp_pct comma15.
construction_emp_pct comma15. otherservices_emp_pct comma15. finance_emp_pct comma15.
health_education_emp_pct comma15. goods_emp_pct comma15. professional_emp_pct comma15.
leisure_emp_pct comma15.;
service_emp_pct=emp_service/total_emp * 100000;
information_emp_pct=emp_information/total_emp * 100000;
trade_emp_pct=emp_trade/total_emp * 100000;
unclassified_emp_pct=emp_unclassified/total_emp * 100000;
manufacturing_emp_pct=emp_manufacturing/total_emp * 100000;
naturalresources_emp_pct=emp_naturnalresources/total_emp * 100000;
construction_emp_pct=emp_construction/total_emp * 100000;
otherservices_emp_pct=emp_otherservices/total_emp * 100000;
finance_emp_pct=emp_finance/total_emp * 100000;
health_education_emp_pct=emp_health_education/total_emp * 100000;
goods_emp_pct=emp_goods/total_emp * 100000;
professional_emp_pct=emp_professional/total_emp * 100000;
leisure_emp_pct=emp_leisure/total_emp * 100000;
est_msa_new=put(est_msa,char5.);

keep leisure_emp_pct service_emp_pct information_emp_pct trade_emp_pct unclassified_emp_pct manufacturing_emp_pct
naturalresources_emp_pct construction_emp_pct otherservices_emp_pct finance_emp_pct
health_education_emp_pct goods_emp_pct professional_emp_pct leisure_emp_pct avg_wg est_msa_new;
rename est_msa_new=est_msa;
run;

proc sort data=covid_pulse out=covid_pulse_sort;
by est_msa;
run;



data pulse_covid_industry replace;
merge covid_pulse_sort (in=ps) industry_calculations (in=ic);
if ps;
by est_msa;
run;

data convert_missing replace;
set pulse_covid_industry;
   array Nums[*] _numeric_;
   array Chars[*] _character_;
   do i = 1 to dim(Nums);
      if Nums[i] = -99 then Nums[i] = '';
      else if Nums[i] = -88 then Nums[i] = '';
      else if Nums[i] = M then Nums[i] = '';
   end;

   do i = 1 to dim(Chars);
      Chars[i] = upcase(Chars[i]);
      if Chars[i] = "-99" then Chars[i] = '';
      else if Chars[i] = "-88" then Chars[i] = '';
      else if Chars[i] = 'M' then Chars[i] = ''; 
   end;
run;

data replace_m replace;
set convert_missing;
array m_replace _numeric_;
do over m_replace;
if m_replace = .M then m_replace='';
end;
run;

proc sql;
create table temp as
select   WEEK,SCRAM,EST_ST,EST_MSA,PWEIGHT,CBSATitle,
         TBIRTH_YEAR,EGENDER,RRACE,EEDUC,MS,THHLD_NUMPER,THHLD_NUMKID,THHLD_NUMADLT,WRKLOSS,EXPCTLOSS,
         ANYWORK,TSPNDFOOD,TSPNDPRPD,HLTHSTATUS,ANXIOUS,WORRY,INTEREST,DOWN,HLTHINS1,TENURE,
         INCOME,totcases,CBSA_POPULATION,cbsa_infectionrate,service_emp_pct,avg_wg,information_emp_pct,trade_emp_pct,unclassified_emp_pct,manufacturing_emp_pct,
         construction_emp_pct,otherservices_emp_pct,finance_emp_pct,health_education_emp_pct,professional_emp_pct,goods_emp_pct,leisure_emp_pct,
         case EST_ST
                when '01' then 'AL'
                  when  '02' then 'AK'
                    when '04' then 'AZ'
                    when '05' then 'AR'
                    when '06' then 'CA'
                   when '08' then 'CO'
                   when '09' then 'CT'
                    when '10' then 'DE'
                   when '11' then 'DC'
                   when  '12' then 'FL'
                   when '13' then 'GA'
                   when '15' then 'HI'
                   when '16' then 'ID'
                   when '17' then 'IL'
                   when '18' then 'IN'
                    when '19' then 'IA'
                   when '20' then 'KS'
                   when  '21' then 'KY'
                   when '22' then 'LA'
                   when '23' then 'ME'
                   when '24' then 'MD'
                   when '25' then 'MA'
                  when  '26' then 'MI'
                  when  '27' then 'MN'
                   when '28' then 'MS'
                   when '29' then 'MO'
                  when  '30' then 'MT'
                   when '31' then 'NE'
                    when '32' then 'NV'
                   when '33' then 'NH'
                   when '34' then 'NJ'
                   when '35' then 'NM'
                   when '36' then 'NY'
                   when '37' then 'NC'
                   when '38' then 'ND'
                   when '39' then 'OH'
                   when '40' then 'OK'
                   when '41' then 'OR'
                   when '42' then 'PA'
                   when  '44' then 'RI'
                   when '45' then 'SC'
                   when '46' then 'SD'
                   when '47' then 'TN'
                   when '48' then 'TX'
                   when '49' then 'UT'
                   when '50' then 'VT'
                   when '51' then 'VA'
                   when '53' then 'WA'
                   when '54' then 'WV'
                   when '55' then 'WI'
                   when '56' then 'WY'
                    end as st_abbrv
from replace_m
where RSNNOWRK not in (1,7) ;
quit;

proc sql ;
create table final_dataset as
 select  a.WEEK,SCRAM,b.state as EST_ST,c.region,a.CBSATitle,EST_MSA,
         TBIRTH_YEAR,2020-TBirth_Year as Age, EGENDER,RRACE,EEDUC,MS,THHLD_NUMPER,THHLD_NUMKID,THHLD_NUMADLT, case WRKLOSS when 1 then 1 when 2 then 0 else 3 end as WRKLOSS,EXPCTLOSS,
         ANYWORK,TSPNDFOOD,TSPNDPRPD,HLTHSTATUS,ANXIOUS,WORRY,INTEREST,DOWN,HLTHINS1,TENURE,
         INCOME,totcases,CBSA_POPULATION,cbsa_infectionrate,service_emp_pct,avg_wg,information_emp_pct,trade_emp_pct,unclassified_emp_pct,manufacturing_emp_pct,
         construction_emp_pct,otherservices_emp_pct,finance_emp_pct,health_education_emp_pct,professional_emp_pct,goods_emp_pct,leisure_emp_pct,
         govt_response_index, containment_health_index, economic_support_index,stringency_index,stringency_legacy_index from temp as a inner join 
 oxford_final as b on a.st_abbrv = b.state and a.week = b.week
 inner join work.region c on a.st_abbrv = c.State_Code
where case WRKLOSS when 1 then 1 when 2 then 0 else 3 end in (1,0);
quit;

proc surveyimpute data=final_dataset method=hotdeck;
	var Age EGENDER RRACE EEDUC MS THHLD_NUMPER THHLD_NUMKID THHLD_NUMADLT  WRKLOSS EXPCTLOSS
         ANYWORK TSPNDFOOD TSPNDPRPD HLTHSTATUS ANXIOUS WORRY INTEREST DOWN HLTHINS1 TENURE INCOME;
    output out = dataset_imputed; 
run;


data final_dataset_imputed;
set dataset_imputed;
drop UnitID ImpIndex; 
run;

PROC EXPORT DATA= final_dataset
OUTFILE= "m:/seane/SASProject/final_dataset.csv" replace
DBMS=CSV ;
PUTNAMES=YES;
RUN;


PROC EXPORT DATA= covidtotals
OUTFILE= "m:/seane/SASProject/covidtotals.csv" replace
DBMS=CSV ;
PUTNAMES=YES;
RUN;

PROC EXPORT DATA= final_dataset_imputed
OUTFILE= "m:/seane/SASProject/final_dataset_imputed.csv" replace
DBMS=CSV ;
PUTNAMES=YES;
RUN;

/* Create Data for Visualization*/
data final_dataset_copy;
RENAME WRKLOSS = JobLoss RRACE = Race EGENDER = Gender EEDUC = Education;
set final_dataset;
run;


proc sql;
create table data_viz as
select SCRAM, WEEK,Region,INCOME,Age, JobLoss, Race, Gender, Education,cbsa_infectionrate,
case JobLoss when 0 then 'Employed' when 1 then 'UnEmployed' end as JOB_LOSS,
case Race when 1 then 'White' when 2 then 'Black' when 3 then 'Asian' when 4 then 'Others' end as RACE_GROUP,
case Gender when 1 then 'Male' when 2 then 'Female' end as GENDER_GROUP,
case Education when 1 then 'Less than HS' when 2 then 'HS dropout' when 3 then 'HS Graduated'
			   when 4 then 'College dropout' when 5 then 'Associate degree' when 6 then 'Bachelor degree' 
			   when 7 then 'Graduate degree'  end as EDUCATION_GROUP,
case when Age between 18 and 25 then '18-25' 
	 when Age between 26 and 35 then '26-35'
	 when Age between 36 and 45 then '36-45'
	 when Age between 46 and 55 then '46-55'
	 when Age between 56 and 65 then '56-65'
	 when Age >65 then '65+' end as AGE_GROUP,
case INCOME when 1 then 'Less than $25,000' when 2 then '$25,000 - $34,999' when 3 then '$35,000 - $49,999'
			when 4 then '$50,000 - $74,999' when 5 then '$75,000 - $99,999' when 6 then '$100,000 - $149,999'
			when 7 then '$150,000 - $199,999' when 8 then '$200,000 and above' end as INCOME_GROUP
from final_dataset_copy;
quit;



/* Job loss by Gender*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ ;
	title height=12pt "Job Loss by Gender";
	vbar GENDER_GROUP / group=JOB_LOSS groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

ods graphics / reset;
title;

/* Job loss by Age*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ ;
	title height=12pt "Job Loss by Age";
	vbar AGE_GROUP / group=JOB_LOSS groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

ods graphics / reset;
title;

/* Job Loss by Income*/

proc sort data=data_viz;
by income;
run;

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group;
	title height=14pt "Job Loss by Income";
	vbar INCOME_GROUP / group=JOB_LOSS groupdisplay=stack stat=percent seglabel ;
	xaxis display=(nolabel) discreteorder=data;
	yaxis display=(nolabel);
	yaxis label="Percentage";
run;

ods graphics / reset;
title;

/*Job Loss by Education*/
proc sort data=data_viz;
by education;
run;

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group;
	title height=14pt "Job Loss by Education";
	vbar EDUCATION_GROUP / group=JOB_LOSS groupdisplay=stack stat=percent seglabel ;
	xaxis display=(nolabel) discreteorder=data;
	yaxis display=(nolabel);
	yaxis label="Percentage";
run;

ods graphics / reset;
title;

/* Job loss by Region*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ ;
	title height=12pt "Job Loss by Region";
	vbar Region / group=JOB_LOSS groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

ods graphics / reset;
title;

/* Age by Region*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group ;
	title height=12pt "Age by Region";
	vbar Region / group=AGE_GROUP groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

/* Race by Region*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group ;
	title height=12pt "Race by Region";
	vbar Region / group=Race_GROUP groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

/* Gender by Region*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group ;
	title height=12pt "Gender by Region";
	vbar Region / group=GENDER_GROUP groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;

/* Education by Region*/
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group ;
	title height=12pt "Education by Region";
	vbar Region / group=Education_GROUP groupdisplay=stack stat=percent seglabel;
	yaxis grid;
	yaxis label="Percentage";
	xaxis display=(nolabel);
run;
/*Region by Education*/
proc sort data=data_viz;
by education;
run;

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group;
	title height=14pt "Job Loss by Education";
	vbar EDUCATION_GROUP / group=Region groupdisplay=stack stat=percent seglabel ;
	xaxis display=(nolabel) discreteorder=data;
	yaxis display=(nolabel);
	yaxis label="Percentage";
run;

ods graphics / reset;
title;


/* Income by Region*/
proc sort data=data_viz;
by income;
run;
ods graphics / reset width=6.4in height=4.8in imagemap;
ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=WORK.DATA_VIZ pctlevel=group;
	title height=14pt "Income by Region";
	vbar INCOME_GROUP / group=Region groupdisplay=stack stat=percent seglabel ;
	xaxis display=(nolabel) discreteorder=data;
	yaxis display=(nolabel);
	yaxis label="Percentage";
run;

ods graphics / reset;
title;

/* T-test for gender*/
PROC TTEST DATA = data_viz;
VAR jobloss;
CLASS GENDER_GROUP;
run;

/* ANOVA test  for more than 2 groups*/
proc anova data= data_viz;
	class AGE_GROUP ;
	model JOBLOSS = AGE_GROUP ;
run;

proc anova data= data_viz;
	class Region ;
	model JOBLOSS = Region ;
run;


/* Correlation matrix of continuos variable*/
proc sort data=final_dataset_copy;
by region;
run;


proc corr data= final_dataset_copy;
	var age cbsa_infectionrate service_emp_pct avg_wg information_emp_pct trade_emp_pct unclassified_emp_pct manufacturing_emp_pct
         construction_emp_pct otherservices_emp_pct finance_emp_pct health_education_emp_pct professional_emp_pct goods_emp_pct leisure_emp_pct
		govt_response_index containment_health_index economic_support_index stringency_index stringency_legacy_index ;
	by region;
run; 