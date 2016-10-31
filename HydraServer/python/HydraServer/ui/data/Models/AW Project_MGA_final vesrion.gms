*******************************************************************************
*Water Capacity Expansion Model
********************************************************************************
********************************************************************************
*1)_________________________DISPLAY (LST FILE ) & DEFINE: SOLVER,_______________
**************specify the DYSPLAY (LST FILE) & DEFINE THE solver****************
option mip=cplex;
**************output results in the lst file*********
Option domlim=100;
Option Limcol=100;
Option Limrow=100;
option solprint=off;
**************running time limit (in seconds)*********
option reslim =99990;
**************relative gap (if zero, it is an global optimum solution***********
option optcr= 0;


*2)_________________________DEFINE SETS AND PARAMETERS__________________________

*3)_________________________IMPORT DATA FROM THE FOLLOWING TXT FILES__________________________
***THESE FILES CONTAIN THE COMMANDS USED TO CALL ALL INPUT DATA IN THE EXCEL FILE 'WRMPtoGAMS15' (SEE FILE GAMS_gdx_import_new1.TXT)
**AND DATA OR SETS USED TO FOR THE LOGICAL CONSTRAINTS (DEPENDENCY, EXCLUSIVITY, ETC., SEE EQUATIONS), FILE (GAMS_gdx_import_CONSTRAINT.TXT)******************************
***YOU SHOULD GET FAMILIAR ON HOW GAMS IMPORTS FILES FROM EXCEL (SEE GAMS_gdx_import_new1 AND GAMS_gdx_import_CONSTRAINT)********************************
$ onempty
*$ include input.txt
$ include EBSD_Input_f.txt


****the alias statements is used to refer to different elements belonging to the same set
ALIAS(SCENARIO,SCENARIO_1);
ALIAS(FF,FFF);


SET LINKS_CODE_2(i,jun_set,j,CODE) ;
    LINKS_CODE_2(i,jun_set,J,CODE)$(SUM(nodes_types,LINKS_CODE_1(i,jun_set,J,nodes_types,CODE)))=YES;
    DISPLAY LINKS_CODE_2;

SET LINKS_CODE_OK(i,jun_set,j,CODE) ;
    LINKS_CODE_OK(i,jun_set,J,CODE)$(SUM(nodes_types,LINKS_CODE_1(i,jun_set,J,nodes_types,CODE)) AND NOT (NOT_USE(CODE)))=YES;
    DISPLAY LINKS_CODE_OK;

*******************SUBSETS BY OPTION TYPE***************************************
************SUBSET FOR DEMAND NODES*********************************************
*$ontext
SET DEMAND_1(I);
    DEMAND_1(I)$SUM(JUN_SET,SUM(j,SUM(links_types,LINK_type(i, jun_set, j ,links_types,'DEMAND'))))=YES;
    DISPLAY DEMAND_1;

SET DEMAND_2(I);
    DEMAND_2(I)$SUM(JUN_SET,SUM(j,SUM(links_types,LINK_type(i, jun_set, j ,links_types,'DEMAND'))))=YES;
    DISPLAY DEMAND_2;

SET DEMAND_TOT(I);
    DEMAND_TOT(I)=DEMAND_1(I)+DEMAND_2(I);
    DISPLAY DEMAND_TOT;
*$offtext

********************************************************************************
************DM******SETS********************************************************
*Metering*Water_Efficiency*LEAKAGE**********************************************

*The following three sets are to distinguish between different type of links i.e. LEAKAGE, Metering, Water_Efficiency
*SET LEAKAGE_SET(I,jun_set,J);
*    LEAKAGE_SET(I,jun_set,J)$SUM(nodes_types,LINK_type(i, jun_set, j ,'LEAKAGE', links_types))=YES ;
	
*    DISPLAY LEAKAGE_SET ;

*SET Metering_SET(I,jun_set,J);
*    Metering_SET(I,jun_set,J)$SUM(nodes_types,LINK_type(i, jun_set, j ,links_types,'Metering'))=YES ;
*    DISPLAY Metering_SET ;

*SET Water_Efficiency_SET(I,jun_set,J);
*    Water_Efficiency_SET(I,jun_set,J)$SUM(nodes_types,LINK_type(i, jun_set, j ,links_types,'Water_Efficiency'))=YES ;	
*    DISPLAY Water_Efficiency_SET ;

* WEFF_MET contains links that has Water_Efficiency or Metering type
SET WEFF_MET(I,jun_set,J);
    WEFF_MET(I,jun_set,J)$(water_efficiencylinks(I,jun_set,J) OR meteringlinks(I,jun_set,J))=YES;
    DISPLAY WEFF_MET;

* DM_TOT_SET contains links that has leakage, water_efficiency, or Metering type
SET DM_TOT_SET(I,jun_set,J);
    DM_TOT_SET(I,jun_set,J)=leakagelinks(I,jun_set,J)
                            +meteringlinks(I,jun_set,J)
                            +water_efficiencylinks(I,jun_set,J);
    DISPLAY DM_TOT_SET;

********************************************************************************************
SET LINK_TYPE_REAL(links_types,I,jun_set,J);
    LINK_TYPE_REAL(links_types,I,jun_set,J)$SUM(nodes_types,LINK_type(i, jun_set, j ,links_types,nodes_types))=YES ;
    DISPLAY LINK_TYPE_REAL ;

* creating a set for links with LEX and ExLinkso type
SET LEX_EXLINKSO_SET(I,jun_set,J);
    LEX_EXLINKSO_SET(I,jun_set,J)$(LINK_TYPE_REAL('LEX',I,JUN_SET,J)+
                                   LINK_TYPE_REAL('ExLinkso',I,JUN_SET,J))=YES;
    DISPLAY LEX_EXLINKSO_SET;

* creating a set for links with ExLinkso type only
SET EXLINKSO_ONLY_SET(I,jun_set,J);
    EXLINKSO_ONLY_SET(I,JUN_SET,J)$( LINK_TYPE_REAL('ExLinkso',I,JUN_SET,J) )=YES;
    DISPLAY EXLINKSO_ONLY_SET;

* creating a set for links with LEX type only
SET LEX_ONLY_SET(I,jun_set,J);
    LEX_ONLY_SET(I,jun_set,J)$( LINK_TYPE_REAL('LEX',I,JUN_SET,J) )=YES;
    DISPLAY LEX_ONLY_SET;
*+++++++++++++++++++++++++++++++++++
*-------------------------------------

* creating a set for links with OptSou type only
SET OPTSOU_ONLY_SET(I,jun_set,J);
    OPTSOU_ONLY_SET(I,jun_set,J)$SUM(links_types,LINK_type(i, jun_set, j ,links_types,'OptSou'))=YES;	
    DISPLAY OPTSOU_ONLY_SET;

* a set containing all links except those with LEX and ExLinkso type
SET LFT_OPTLINKSO_SET(I,jun_set,J);
    LFT_OPTLINKSO_SET(I,jun_set,J)$(LINKS(I,jun_set,J) AND NOT LEX_EXLINKSO_SET(I,jun_set,J))=YES;
    DISPLAY LFT_OPTLINKSO_SET;
*****************************************************************************************************

SET  LEFT_RIGHT_DEP_SET_2(DEPENDENCY_SET,CODE,LEFT_RIGHT_SET,I,jun_set,J);
     LEFT_RIGHT_DEP_SET_2(DEPENDENCY_SET,CODE,LEFT_RIGHT_SET,I,jun_set,J)$
     ( LEFT_RIGHT_DEP_SET_1(DEPENDENCY_SET,CODE,LEFT_RIGHT_SET) AND LINKS_CODE_OK(I,jun_set,J,CODE)  )=YES ;
     Display LEFT_RIGHT_DEP_SET_2;

SET  LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,LEFT_RIGHT_SET,I,jun_set,J);
     LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,LEFT_RIGHT_SET,I,jun_set,J)$
     SUM(CODE,LEFT_RIGHT_DEP_SET_2(DEPENDENCY_SET,CODE,LEFT_RIGHT_SET,I,jun_set,J) )=YES ;
     Display LEFT_RIGHT_DEP_SET_TOT;
	 

********************************************************************************
********************************************************************************

SET  EXCL_SET_2(EXCLUSIVITY_SET,CODE,I,jun_set,J);
     EXCL_SET_2(EXCLUSIVITY_SET,CODE,I,jun_set,J)$
     ( EXCL_SET_1(EXCLUSIVITY_SET,CODE) AND LINKS_CODE_OK(I,jun_set,J,CODE)  )=YES ;
     Display EXCL_SET_2;

SET  EXCL_SET_TOT(EXCLUSIVITY_SET,I,jun_set,J);
     EXCL_SET_TOT(EXCLUSIVITY_SET,I,jun_set,J)$
     SUM(CODE,EXCL_SET_2(EXCLUSIVITY_SET,CODE,I,jun_set,J) )=YES ;
     Display EXCL_SET_TOT;


PARAMETER       MIN_CAP_OPT_2(I,JUN_SET,J,CODE) MINIMUM CAPACITY (PART 2) FOR THE OPTIONAL NODES- ASSIGNED TO THE NODE'S NAME ('CODE') AND INTERCONNECTIVITY (I-JUN_SET-J);
                MIN_CAP_OPT_2(I,JUN_SET,J,CODE)$LINKS_CODE_OK(I,jun_set,J,CODE)=MIN_CAP_OPT_1(CODE,'MIN_OPT');
                DISPLAY MIN_CAP_OPT_2;

PARAMETER       MIN_CAP_OPT(I,JUN_SET,J) MINIMUM CAPACITY (PART 2) FOR THE OPTIONAL NODES- ASSIGNED ONLY TO THE INTERCONNECTIVITY (I-JUN_SET-J) ;
                MIN_CAP_OPT(I,JUN_SET,J)=SUM(CODE, MIN_CAP_OPT_2(I,JUN_SET,J,CODE));
                DISPLAY MIN_CAP_OPT;

ALIAS(YR,TT);

PARAMETERS  TIME(yr) "yearly vector to discount annual costs OVER THE PLANNING PERIOD YR" ;
            TIME(yr) = ord(yr)-1;

PARAMETER   TIME_FF(FF) "yearly vector to discount annual costs OVER TIME INDEX FF" ;
            TIME_FF(FF)=ord(FF)-1;


SCALAR     present_worth_factor USED TO DERIVE THE NPV (ONE UNIQUE CASH FLOW) OF A SERIES OF 'N' EQUAL PAYMENTS OCCURING INTO THE FUTURE ;
           present_worth_factor= (POWER((1+IR),N)-1)/(IR*POWER((1+IR),N));

PARAMETER MAX_CAPACITY_OPT_NOT_DM(I,JUN_SET,J,SCENARIO) MAXIMUM CAPACITY FOR ALL OPTIONAL NODES AND LINKS- SET 'LFT_OPTLINKSO_SET' (WITHOUT INCLUDING DEMAND MANAGEMENT NODES);
          MAX_CAPACITY_OPT_NOT_DM(I,JUN_SET,J,SCENARIO)$LFT_OPTLINKSO_SET(I,JUN_SET,J) =
                           SUM(SCENARIO_TS_PAR$(ORD(SCENARIO) EQ ORD(SCENARIO_TS_PAR)),
*                           NODES_TS_data('2015-16',SCENARIO_TS_PAR,I)$LFT_OPTLINKSO_SET(I,JUN_SET,J)
						    Max_annual_capacity ('2015-16', i, scenario_ts_par)$LFT_OPTLINKSO_SET(I,JUN_SET,J)
*                           +LINKS_TS_data('2015-16',SCENARIO_TS_PAR,I,JUN_SET,J) )
						   + Maxflow ('2015-16', I, JUN_SET, J, SCENARIO_TS_PAR) )

                           +SUM(SCENARIO_TS_PAR$((ORD(SCENARIO_TS_PAR)+2 EQ ORD(SCENARIO))
                                                  AND (ORD(SCENARIO) EQ 3)),
*                           NODES_TS_data('2015-16',SCENARIO_TS_PAR,I)$LFT_OPTLINKSO_SET(I,JUN_SET,J)
						   Max_annual_capacity ('2015-16', i, scenario_ts_par)$LFT_OPTLINKSO_SET(I,JUN_SET,J)
*                           +LINKS_TS_data('2015-16',SCENARIO_TS_PAR,I,JUN_SET,J) ) ;
    						   + Maxflow ('2015-16', I, JUN_SET, J, SCENARIO_TS_PAR) );
DISPLAY MAX_CAPACITY_OPT_NOT_DM;


PARAMETER MAX_CAPACITY_EXT_NODES(I,JUN_SET,J,SCENARIO) MAXIMUM CAPACITY FOR ALL EXISTING NODES (SET EXLINKSO) ;
          MAX_CAPACITY_EXT_NODES(I,JUN_SET,J,SCENARIO)$(EXLINKSO_ONLY_SET(I,jun_set,J)) =
                           SUM(SCENARIO_TS_PAR$(ORD(SCENARIO) EQ ORD(SCENARIO_TS_PAR)),
*                               NODES_TS_data('2015-16',SCENARIO_TS_PAR,I)
							   Max_annual_capacity ('2015-16', i, scenario_ts_par)
*                               +LINKS_TS_data('2015-16',SCENARIO_TS_PAR,I,JUN_SET,J)
                               )

                           +SUM(SCENARIO_TS_PAR$((ORD(SCENARIO_TS_PAR)+2 EQ ORD(SCENARIO))
                                                  AND (ORD(SCENARIO) EQ 3)),
*                           NODES_TS_data('2015-16',SCENARIO_TS_PAR,I)
						   Max_annual_capacity ('2015-16', i, scenario_ts_par)
*                           +LINKS_TS_data('2015-16',SCENARIO_TS_PAR,I,JUN_SET,J)
                               ) ;
DISPLAY MAX_CAPACITY_EXT_NODES;


PARAMETER MAX_CAPACITY_DM(I,JUN_SET,J) MAXIMUM CAPACITY FOR DEMAND MANAGEMENT NODES ;
          MAX_CAPACITY_DM(I,JUN_SET,J)=DM_DATA(I,'WafuDM','1')$DM_TOT_SET(I,jun_set,J);
          DISPLAY MAX_CAPACITY_DM;


PARAMETER TS_PAR_DM(I,JUN_SET,J,COST_PAR_PAR) COST DATA (FIXED AND VARIABLE OPERATING COST AND CAPITAL COST-SEE SET 'COST_PAR_PAR') FOR DEMAND MANAGEMENT NODES;
          TS_PAR_DM(I,JUN_SET,J,COST_PAR_PAR)=DM_DATA(I,COST_par_par,'1')$DM_TOT_SET(I,jun_set,J);
          DISPLAY TS_PAR_DM;


PARAMETER DI_THR_PAR(YR,SCENARIO,I) SUM OF DISTRIBUTION INPUT (DEMAND) AND TARGET HEADROOM (THR);
          DI_THR_PAR(YR,SCENARIO,I)$DEMAND_APPLY(I)=
                     SUM( DEMAND_TS_PAR$(ORD(DEMAND_TS_PAR) LE 2),
                          DEMAND_TS_DATA(YR,SCENARIO,DEMAND_TS_PAR,I) );
           DISPLAY DI_THR_PAR ;

PARAMETER INITIAL_DEFICIT_EXTLINK(YR,SCENARIO,J) FOR CHECKING PURPOSES ONLY-DEFICIT AT EACH WRZ- IF NONE OF THE OPETIONAL SCHEMES IS ACTIVATED;
          INITIAL_DEFICIT_EXTLINK(YR,SCENARIO,J)$DEMAND_APPLY(J)=
          SUM( (I,JUN_SET)$EXLINKSO_ONLY_SET(I,JUN_SET,J),
          MAX_CAPACITY_EXT_NODES(I,JUN_SET,J,SCENARIO) )

          +sum((I,JUN_SET)$LEX_ONLY_SET(I,JUN_SET,J),
               MIN_MAX_CAP_EXT_LINK(I,JUN_SET,J,'link_min_capacity',SCENARIO))
          -sum((JUN_SET,I)$LEX_ONLY_SET(J,JUN_SET,I),
                MIN_MAX_CAP_EXT_LINK(J,JUN_SET,I,'link_min_capacity',SCENARIO))

          +DEMAND_TS_DATA(YR,SCENARIO,'CHANGE_DO',J)
          -DEMAND_TS_DATA(YR,SCENARIO,'PL_RWLOU',J)
          -DI_THR_PAR(YR,SCENARIO,J);
DISPLAY  INITIAL_DEFICIT_EXTLINK;



SET DEFICIT_WRZ_AMP6(I)  SET OF DEMAND NODES WHICH HAVE A DEFICIT-I.E. A NEGATIVE VALUE IN PARAMETER 'INITIAL_DEFICIT_EXTLINK' ;
LOOP((YR,SCENARIO)$(ORD(YR) le 5),
    DEFICIT_WRZ_AMP6(I)$(DEMAND_APPLY(I) AND(INITIAL_DEFICIT_EXTLINK(YR,SCENARIO,I) LT 0))=YES;);
    DISPLAY DEFICIT_WRZ_AMP6;

*PARAMETER DEMAND_TOT_PAR(YR,SCENARIO,*);
*          DEMAND_TOT_PAR(YR,SCENARIO,I) =
*                           SUM(SCENARIO_TS_PAR$(ORD(SCENARIO) EQ ORD(SCENARIO_TS_PAR)),
*                           DEMAND_TS_data(yr,SCENARIO,DEMAND_TS_PAR,I)$LINKS(I,JUN_SET,J) )
*
*                           +SUM(SCENARIO_TS_PAR$((ORD(SCENARIO_TS_PAR)+2 EQ ORD(SCENARIO))
*                                                  AND (ORD(SCENARIO) EQ 3)),
*                            DEMAND_TS_data(yr,SCENARIO,DEMAND_TS_PAR,I)$LINKS(I,JUN_SET,J) )  ;

SET LFT_SUPPLY_ONLY_OPT(I,JUN_SET,J) SET OF OPTIONAL NODES AND OPTIONAL LINKS;
    LFT_SUPPLY_ONLY_OPT(I,JUN_SET,J)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) and not DM_TOT_SET(I,JUN_SET,J))=YES;
    DISPLAY LFT_SUPPLY_ONLY_OPT;


PARAMETER AVAILABILITY(I,JUN_SET,J) FIRST YEAR OF AVAILABILITY OF OPTIONAL SCHEMES;
          AVAILABILITY(I,JUN_SET,J)$sum(links_types,LINK_TYPE_REAL(links_types,I,JUN_SET,J))=
                         (NODES_PAR_DATA(I,'AvalYear')$LFT_SUPPLY_ONLY_OPT(I,JUN_SET,J)
                          +LINKS_PAR_DATA(I,JUN_SET,J,'AvalYear')$LFT_OPTLINKSO_SET(I,jun_set,J)
                          +TS_PAR_DM(I,JUN_SET,J,'AvalYear')$DM_TOT_SET(I,JUN_SET,J) )
*                          +2015$((TS_AND_PAR_DM(I,JUN_SET,J,'AvalYear') eq 0) AND DM_TOT_SET(I,JUN_SET,J))
;
DISPLAY AVAILABILITY;


*5)_________________________AGGREGATING COST DATA_______________________________
****ALL COSTS (SEE GREEN WRITING BELOW-I.E. FixedS,FixedOp) ARE RE-AGGREGATED INTO THREE ONLY CATEGORIES SUCH
*   AS CAPITAL COSTS (SET CAPEX_SET), FIXED ANNUAL COSTS IN K£/YR (SET FOPEX_SET), VARIABLE COSTS IN P/M^3 (SET VOPEX_SET)
SET CAPEX_SET(COST_PAR_PAR)/ FixedS /;
SET FOPEX_SET(COST_PAR_PAR)/ FixedOp,OpSav,EnvFixed,CarbFix /;
SET VOPEX_SET(COST_PAR_PAR)/ VarS,VarOp,EnvVar,CarbVar /;


PARAMETER    CAPEX(I,JUN_SET,J)  SUM OF ALL COSTS IN SET 'CAPEX_SET' ABOVE ;
             CAPEX(I,JUN_SET,J)$sum(links_types,LINK_TYPE_REAL(links_types,I,JUN_SET,J))
                              =( SUM(COST_PAR_PAR$CAPEX_SET(COST_PAR_PAR),
                              NODES_PAR_DATA(I,COST_PAR_PAR)$LINKS(I,JUN_SET,J)
                              +LINKS_PAR_DATA(I,JUN_SET,J,COST_PAR_PAR)
                              +TS_PAR_DM(I,JUN_SET,J,COST_PAR_PAR))  )  ;
             DISPLAY CAPEX;

PARAMETERS   FOPEX(I,JUN_SET,J) SUM OF ALL COSTS IN SET 'FOPEX_SET' ABOVE;
             FOPEX(I,JUN_SET,J)$sum(links_types,LINK_TYPE_REAL(links_types,I,JUN_SET,J))
                              =( SUM(COST_PAR_PAR$FOPEX_SET(COST_PAR_PAR),
                              NODES_PAR_DATA(I,COST_PAR_PAR)$LINKS(I,JUN_SET,J)
                              +LINKS_PAR_DATA(I,JUN_SET,J,COST_PAR_PAR)
                              +TS_PAR_DM(I,JUN_SET,J,COST_PAR_PAR)) );
             DISPLAY FOPEX;

PARAMETERS   VOPEX(I,JUN_SET,J) SUM OF ALL COSTS IN SET 'VOPEX_SET' ABOVE;
             VOPEX(I,JUN_SET,J)$sum(links_types,LINK_TYPE_REAL(links_types,I,JUN_SET,J))
                              =( SUM(COST_PAR_PAR$VOPEX_SET(COST_PAR_PAR),
                              NODES_PAR_DATA(I,COST_PAR_PAR)$LINKS(I,JUN_SET,J)
                              +LINKS_PAR_DATA(I,JUN_SET,J,COST_PAR_PAR)
                              +TS_PAR_DM(I,JUN_SET,J,COST_PAR_PAR)) );
             DISPLAY VOPEX;


*in case of links exporting or importing water to (or from) external WRZs, the mass balance equation does
* not apply to these zones, therefore a set is applied (DEMAND_NOT_APPLY) which excludes external WRZs

SET DEMAND_NOT_APPLY(I);
    DEMAND_NOT_APPLY(I)$(DEMAND_TOT(I) AND NOT DEMAND_APPLY(I))=YES;
    DISPLAY DEMAND_NOT_APPLY;

****existing links that export water from deman node i to other nodes j
SET EXISTING_EXPORT_EXTERNAL(I,JUN_SET,J);
    EXISTING_EXPORT_EXTERNAL(I,JUN_SET,J)$(LEX_EXLINKSO_SET(I,JUN_SET,J) AND DEMAND_APPLY(I) AND DEMAND_NOT_APPLY(J))=YES;
    DISPLAY EXISTING_EXPORT_EXTERNAL;

****optional nodes used to represent export to external demand nodes
SET OPTIONAL_EXPORT_EXTERNAL(I,JUN_SET,J);
    OPTIONAL_EXPORT_EXTERNAL(I,JUN_SET,J)$(DEMAND_APPLY(I) AND OPTSOU_ONLY_SET(I,JUN_SET,J))=YES;
    DISPLAY OPTIONAL_EXPORT_EXTERNAL;

*5)_________________________SETS USED FOR THE LOGICAL CONSTRAINTS__________________________

********************************************************************************
****SETS CREATED FOR THE MUTUALLY DEPENDENT CONSTRAINT**************************
*$ONTEXT

*****IDENTIFICATIVE NUMBER FOR THE DEPENDENCY CONSTRAINT*************************
PARAMETER NUM_DEP(DEPENDENCY_SET);
          NUM_DEP(DEPENDENCY_SET)=
          SUM(LEFT_RIGHT_SET,SUM((I,JUN_SET,J)$
          LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,LEFT_RIGHT_SET,I,jun_set,J),1))-1;
DISPLAY   NUM_DEP;

*******SET OF NODES ON THE RIGHT OF THE DEPENDENCY CONSTRAINT EQUATION*************
SET       SUB_SET_RIGHT_DEPENDENCY(DEPENDENCY_SET);
          LOOP ((I,JUN_SET,J),
                 SUB_SET_RIGHT_DEPENDENCY(DEPENDENCY_SET)
                 $LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,'DEP_RIGHT',I,jun_set,J) =yes ;);
DISPLAY   SUB_SET_RIGHT_DEPENDENCY;

*****FOR CHECKING PURPOSES: SET OR OPTIOANL LINKS AN NOSED (EXCLUDING THE DEMAND MANAGEMENT ONE)*****
SET CHECK_OPTSOU_AND_NOT_DM(I,JUN_SET,J);
    CHECK_OPTSOU_AND_NOT_DM(I,JUN_SET,J)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) AND NOT DM_TOT_SET(I,JUN_SET,J))=YES;
    DISPLAY CHECK_OPTSOU_AND_NOT_DM ;
*$OFFTEXT
********************************************************************************
$ontext
set plot_1(SCENARIO,I,JUN_SET,J,FF,YR);
    plot_1(SCENARIO,I,JUN_SET,J,FF,YR)$((CARD(YR)-ORD(YR)+1+ORD(FF) EQ N) AND LFT_OPTLINKSO_SET(I,JUN_SET,J))=yes;

set plot_2(SCENARIO,I,JUN_SET,J,FF,FFF);
    plot_2(SCENARIO,I,JUN_SET,J,FF,FFF)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) AND (ORD(FF) GE ORD(FFF)))=yes;



Execute_unload "CHECK_SET_EQ.gdx" plot_1,plot_2,LFT_OPTLINKSO_SET;

execute 'gdxxrw.exe CHECK_SET_EQ.gdx SET=plot_1 rng=plot_1! Rdim=6 SQ=N'
execute 'gdxxrw.exe CHECK_SET_EQ.gdx SET=plot_2 rng=plot_2! Rdim=4 SQ=N'
execute 'gdxxrw.exe CHECK_SET_EQ.gdx SET=LFT_OPTLINKSO_SET rng=LFT_OPTLINKSO_SET! Rdim=3 SQ=N'
$offtext
$ONTEXT
Execute_unload "CHECK_INPUT.gdx"
MAX_CAPACITY_OPT_NOT_DM, MAX_CAPACITY_EXT_NODES, TS_PAR_DM,  DI_THR_PAR
MAX_CAPACITY_DM ,INITIAL_DEFICIT_EXTLINK,AVAILABILITY,NUM_DEP
MIN_CAP_OPT, MIN_MAX_CAP_EXT_LINK,FOPEX,VOPEX,CAPEX

DEFICIT_WRZ_AMP6,LFT_SUPPLY_ONLY_OPT,DEMAND_NOT_APPLY,
EXISTING_EXPORT_EXTERNAL,OPTIONAL_EXPORT_EXTERNAL,
SUB_SET_RIGHT_DEPENDENCY,CHECK_OPTSOU_AND_NOT_DM


execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=DI_THR_PAR rng=DI_THR_PAR! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=MAX_CAPACITY_OPT_NOT_DM rng=MAX_CAPACITY_OPT_NOT_DM! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx par=MIN_CAP_OPT rng=MIN_CAP_OPT! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=MAX_CAPACITY_EXT_NODES rng=MAX_CAPACITY_EXT_NODES! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=MIN_MAX_CAP_EXT_LINK rng=MIN_MAX_CAP_EXT_LINK! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=TS_PAR_DM rng=TS_PAR_DM! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=CAPEX rng=CAPEX! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=FOPEX rng=FOPEX! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=VOPEX rng=VOPEX! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=AVAILABILITY rng=AVAILABILITY! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=INITIAL_DEFICIT_EXTLINK rng=INITIAL_DEFICIT_EXTLINK! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx PAR=MAX_CAPACITY_DM rng=MAX_CAPACITY_DM! Rdim=3 SQ=N'

execute 'gdxxrw.exe CHECK_INPUT.gdx SET=DEFICIT_WRZ_AMP6 rng=DEFICIT_WRZ_AMP6! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=LFT_SUPPLY_ONLY_OPT rng=LFT_SUPPLY_ONLY_OPT! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=DEMAND_NOT_APPLY rng=DEMAND_NOT_APPLY! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=EXISTING_EXPORT_EXTERNAL rng=EXISTING_EXPORT_EXTERNAL! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=OPTIONAL_EXPORT_EXTERNAL rng=OPTIONAL_EXPORT_EXTERNAL! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=SUB_SET_RIGHT_DEPENDENCY rng=SUB_SET_RIGHT_DEPENDENCY! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_INPUT.gdx SET=CHECK_OPTSOU_AND_NOT_DM rng=CHECK_OPTSOU_AND_NOT_DM! Rdim=3 SQ=N'



Execute_unload "CHECK_CHECK.gdx"
NO_DUPL,i,jun_set, yr, COST_par_par,Not_Prefered, CODE,links_types,nodes_types
DEMAND_1,DEMAND_2,DEMAND_TOT,DEMAND_APPLY
leakagelinks,meteringlinks,meteringlinks,WEFF_MET,DM_TOT_SET,LEX_EXLINKSO_SET,EXLINKSO_ONLY_SET,LEX_ONLY_SET,OPTSOU_ONLY_SET,LFT_OPTLINKSO_SET,LINKS
LINKS_CODE_2,LINKS_CODE_OK,LINK_type,LINKS_CODE_1
*nodes_TS_data,nodes_par_data,DEMAND_TS_data,LINKS_TS_data,DM_DATA,LINKS_par_data
Max_annual_capacity,nodes_par_data,DEMAND_TS_data,Maxflow,DM_DATA,LINKS_par_data

;

execute 'gdxxrw.exe CHECK_CHECK.gdx SET=NO_DUPL rng=NO_DUPL! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=i rng=i! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=jun_set rng=jun_set! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=yr rng=yr! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=COST_par_par rng=COST_par_par! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=Not_Prefered rng=Not_Prefered! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=CODE rng=CODE! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=links_types rng=links_types! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=nodes_types rng=nodes_types! Rdim=1 SQ=N'

execute 'gdxxrw.exe CHECK_CHECK.gdx SET=DEMAND_1 rng=DEMAND_1! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=DEMAND_2 rng=DEMAND_2! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=DEMAND_TOT rng=DEMAND_TOT! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=DEMAND_APPLY rng=DEMAND_APPLY! Rdim=1 SQ=N'

execute 'gdxxrw.exe CHECK_CHECK.gdx SET=leakagelinks rng=leakagelinks! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=meteringlinks rng=meteringlinks! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=meteringlinks rng=meteringlinks! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=WEFF_MET rng=WEFF_MET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=DM_TOT_SET rng=DM_TOT_SET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LEX_EXLINKSO_SET rng=LEX_EXLINKSO_SET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=EXLINKSO_ONLY_SET rng=EXLINKSO_ONLY_SET! Rdim=3 SQ=N'

execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LEX_ONLY_SET rng=LEX_ONLY_SET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=OPTSOU_ONLY_SET rng=OPTSOU_ONLY_SET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LFT_OPTLINKSO_SET rng=LFT_OPTLINKSO_SET! Rdim=3 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LINKS rng=LINKS! Rdim=3 SQ=N'

execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LINKS_CODE_2 rng=LINKS_CODE_2! Rdim=4 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LINKS_CODE_OK rng=LINKS_CODE_OK! Rdim=4 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LINK_type rng=LINK_type! Rdim=5 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx SET=LINKS_CODE_1 rng=LINKS_CODE_1! Rdim=5 SQ=N'

*execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=nodes_TS_data rng=nodes_TS_data! Rdim=2 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=Max_annual_capacity rng=Max_annual_capacity! Rdim=2 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=nodes_par_data rng=nodes_par_data! Rdim=1 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=DEMAND_TS_data rng=DEMAND_TS_data! Rdim=3 SQ=N'
*execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=LINKS_TS_data rng=LINKS_TS_data! Rdim=2 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=Maxflow rng=Maxflow! Rdim=2 SQ=N'

execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=DM_DATA rng=DM_DATA! Rdim=2 SQ=N'
execute 'gdxxrw.exe CHECK_CHECK.gdx PAR=LINKS_par_data rng=LINKS_par_data! Rdim=3 SQ=N'
$OFFTEXT
********************************************************************************
VARIABLE               COST,DEF_TOT                                   ;

POSITIVE VARIABLE      Q(I,JUN_SET,J,SCENARIO,YR) EXTENT OF USE OF SCHEME I IN YEAR YR (ML PER DAY)
                       QQQ(I,JUN_SET,J,SCENARIO,FF) EQUAL TO Q AT LAST YEAR OF SET YR- FOR AS MANY YEARS AS NECESSARY TO REACH A NUMBER OF N (80) STARTING FROM THE FIRST YEAR OF ACTIVATION OF THE SCHEME
                       QQ(I,JUN_SET,J,SCENARIO,FF), Q_AL(I,JUN_SET,J,SCENARIO,YR) (INTERMEDIATE VARIABLES USED TO DEFINE QQQ)

                       ALPHA(YR,SCENARIO,I) PERCENTAGE OF DEMAND SATISFACTION
                       BETHA(YR,SCENARIO,I,JUN_SET,J) DEFICIT OF WATER (ML PER DAY) CAUSED BY LOWER BOUNDS CONSTRAINTS ON THE EXISTING LINKS;

BINARY VARIABLE        AL(I,JUN_SET,J,YR) BINARY VARIABLE FOR ACTIVATION OF SCHEMES ;

EQUATIONS
MIN_COST NET PRESENT VALUE MINIMUM COST
MIN_DEF  MINIMUM DEFICIT IN THE EXISTING LINKS

QQ_EQ_LINEARISE(SCENARIO,I,JUN_SET,J,YR) EQUATION USED TO DEFINE QQQ (SEE REPORT)
QQ_EQ(SCENARIO,I,JUN_SET,J,FF,YR) EQUATION USED TO DEFINE QQQ (SEE REPORT)
QQQ_EQ(SCENARIO,I,JUN_SET,J,FF) EQUATION USED TO DEFINE QQQ (SEE REPORT)

MASS_BALANCE_EQ(SCENARIO,yr,I) MASS BALANCE APPLIES AT EACH DEMAND NODE AND SUPPLY JUNCTON NODE
MAX_CAPACITY_EQ_NOT_DM(SCENARIO,YR,I,JUN_SET,J) UPPER BOUND CONSTRAINTS ON OPTIONAL NODES AND OPTIONAL LINKS
CAPACITY_EQ_JUST_DM(SCENARIO,YR,I,JUN_SET,J) USER DEFINED SAVING FROM DEMAND MANAGEMENT NODES (THIS IS ACCOUNTED STARTING FROM THE FIRST YEAR ACTIVATION OF THE SCHEME)
MIN_CAP_EXT_LINK_EQ(I,JUN_SET,J,SCENARIO,YR) MINIMUM BOUND ON FLOW IN THE EXISTING LINKS
MIN_CAP_OPT_NODE_lft_EQ(I,JUN_SET,J,SCENARIO,YR) MINIMUM BOUND ON FLOW IN THE OPTIONAL LINKS AND OPTIONAL NODES
DEPENDENCY_EQ(YR,DEPENDENCY_SET)  DEPENDENCY CONSTRAINT (E.G. NODE A AND NODE B MUST BOTH BE SELECTED)
EXCLUSIVITY_EQ(YR,EXCLUSIVITY_SET) EXCLUSIVITY CONSTRAINT (E.G. ONLY ONE NODE AMONGST NODE A AND NODE B MUST BE SELECTED)
CONTINUITY_EQ(YR,I,JUN_SET,J)   ONCE A NODE IS SELECTED- ITS BINARY VARIABLE MUST BE EQUAL TO ONE UNTILL THE END OF THE PLANNING HORIZON (SET YR);

**********************************CODE*********************************************************************************************************************************************
MIN_DEF.. DEF_TOT=e=

SUM(YR,SUM(SCENARIO,SUM(I$DEMAND_APPLY(I),
       ALPHA(YR,SCENARIO,I) )))
+SUM(YR,
     SUM(SCENARIO,
     SUM((I,JUN_SET,J)$LEX_ONLY_SET(I,JUN_SET,J),
          BETHA(YR,SCENARIO,I,JUN_SET,J))))

;
***UPPER BOUND ON VARIABLE ALPHA****************************************************************
ALPHA.UP(YR,SCENARIO,I)$DEMAND_APPLY(I)=1;
***************************************************************************************************
MASS_BALANCE_EQ(SCENARIO,yr,I)$DEMAND_APPLY(I)..
            SUM((J,JUN_SET)$LINKS(J,JUN_SET,I), Q(J,JUN_SET,I,SCENARIO,YR))
            -SUM((JUN_SET,J)$LINKS(I,JUN_SET,J), Q(I,JUN_SET,J,SCENARIO,YR))
            +DEMAND_TS_DATA(YR,SCENARIO,'Change_DO',I)
            -DEMAND_TS_DATA(YR,SCENARIO,'PL_RWLOU',I)
            =E=ALPHA(YR,SCENARIO,I)*DI_THR_PAR(YR,SCENARIO,I);

MAX_CAPACITY_EQ_NOT_DM(SCENARIO,YR,I,JUN_SET,J)$(LINKS(I,JUN_SET,J)and not DM_TOT_SET(I,JUN_SET,J)) ..
               Q(I,JUN_SET,J,SCENARIO,YR)=L=
                       (MAX_CAPACITY_OPT_NOT_DM(I,JUN_SET,J,SCENARIO)
*                        +MAX_CAPACITY_DM(I,JUN_SET,J)
                        )
                        *(  AL(I,JUN_SET,J,YR)$LFT_OPTLINKSO_SET(I,JUN_SET,J)  )
                        +MAX_CAPACITY_EXT_NODES(I,JUN_SET,J,SCENARIO)
                        +MIN_MAX_CAP_EXT_LINK(I,JUN_SET,J,'link_max_capacity',SCENARIO)
 ;

***UPPER BOUND ON VARIABLE BETHA****************************************************************
BETHA.UP(YR,SCENARIO,I,JUN_SET,J)$LEX_ONLY_SET(I,JUN_SET,J)=1;
***************************************************************************************************
MIN_CAP_EXT_LINK_EQ(I,JUN_SET,J,SCENARIO,YR)$LEX_ONLY_SET(I,JUN_SET,J)..
             Q(I,JUN_SET,J,SCENARIO,YR)=G=
              MIN_MAX_CAP_EXT_LINK(I,JUN_SET,J,'link_min_capacity',SCENARIO)
             *BETHA(YR,SCENARIO,I,JUN_SET,J);

MIN_CAP_OPT_NODE_lft_EQ(I,JUN_SET,J,SCENARIO,YR)$LFT_OPTLINKSO_SET(I,JUN_SET,J)..
  Q(I,JUN_SET,J,SCENARIO,YR)=G=MIN_CAP_OPT(I,JUN_SET,J)*AL(I,JUN_SET,J,YR);

*$ONTEXT
MIN_COST.. COST=e=
SUM(YR,
      (
    SUM((I,JUN_SET,J)$LFT_OPTLINKSO_SET(I,JUN_SET,J),

       (    CAPEX(I,JUN_SET,J)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) AND not WEFF_MET(I,JUN_SET,J))
           +(CAPEX(I,JUN_SET,J)*present_worth_factor)$WEFF_MET(I,JUN_SET,J)
           +(FOPEX(I,JUN_SET,J)*present_worth_factor)
        )

          *(AL(I,JUN_SET,J,YR)-AL(I,JUN_SET,J,YR-1))


            +SUM( SCENARIO,(T_SCEN(SCENARIO)*VOPEX(I,JUN_SET,J)
                            *Q(I,JUN_SET,J,SCENARIO,YR))
                  /SUM(SCENARIO_1,T_SCEN(SCENARIO_1))   )
         )
       ) /POWER(1+DR,TIME(yr))

    )
$ONTEXT
+SUM(FF,
         SUM((I,JUN_SET,J)$LFT_OPTLINKSO_SET(I,JUN_SET,J),

         SUM( SCENARIO,(T_SCEN(SCENARIO)*VOPEX(I,JUN_SET,J)
                        *QQQ(I,JUN_SET,J,SCENARIO,FF))
               /SUM(SCENARIO_1,T_SCEN(SCENARIO_1))   )
          /POWER(1+DR,CARD(YR)+TIME_FF(FF)-1)

              )
      )
$offtext
;

QQ_EQ_LINEARISE(SCENARIO,I,JUN_SET,J,YR)$LFT_OPTLINKSO_SET(I,JUN_SET,J)..
                 SUM(TT$(ORD(TT)EQ CARD(TT)), Q(I,JUN_SET,J,SCENARIO,TT))
                  -M*(1-(AL(I,JUN_SET,J,YR)-AL(I,JUN_SET,J,YR-1)))
                  =L=Q_AL(I,JUN_SET,J,SCENARIO,YR);

QQ_EQ(SCENARIO,I,JUN_SET,J,FF,YR)$((CARD(YR)-ORD(YR)+1+ORD(FF) EQ N) AND LFT_OPTLINKSO_SET(I,JUN_SET,J))..
                 QQ(I,JUN_SET,J,SCENARIO,FF)=E=Q_AL(I,JUN_SET,J,SCENARIO,YR);

QQQ_EQ(SCENARIO,I,JUN_SET,J,FFF)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) )..
                 QQQ(I,JUN_SET,J,SCENARIO,FFF)=G=sum(ff$(ORD(FF) GE ORD(FFF)),QQ(I,JUN_SET,J,SCENARIO,FF));



CAPACITY_EQ_JUST_DM(SCENARIO,YR,I,JUN_SET,J)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) AND DM_TOT_SET(I,JUN_SET,J))..
               Q(I,JUN_SET,J,SCENARIO,YR)=E=
               MAX_CAPACITY_DM(I,JUN_SET,J)*AL(I,JUN_SET,J,YR);


DEPENDENCY_EQ(YR,DEPENDENCY_SET)..
                  SUM((I,JUN_SET,J)$LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,'LEFT_DEP',I,jun_set,J), AL(I,JUN_SET,J,YR))=E=
                  SUM((I,JUN_SET,J)$LEFT_RIGHT_DEP_SET_TOT(DEPENDENCY_SET,'DEP_RIGHT',I,jun_set,J), AL(I,JUN_SET,J,YR))
                  /NUM_DEP(DEPENDENCY_SET)$SUB_SET_RIGHT_DEPENDENCY(DEPENDENCY_SET);

EXCLUSIVITY_EQ(YR,EXCLUSIVITY_SET)..
                  SUM((I,JUN_SET,J)$EXCL_SET_TOT(EXCLUSIVITY_SET,I,JUN_SET,J), AL(I,JUN_SET,J,YR))=L=1;

CONTINUITY_EQ(YR,I,JUN_SET,J)$
                 ((ORD(YR) LT CARD(YR)) AND LFT_OPTLINKSO_SET(I,JUN_SET,J))..
                  AL(I,JUN_SET,J,YR+1)=G=AL(I,JUN_SET,J,YR);

*******************************FORCING OPTIONS NOT TO BE SELECTED BEFORE THEIR FIRST YEAR OF AVAILABILITY **************************

AL.FX(I,JUN_SET,J,YR)$(LFT_OPTLINKSO_SET(I,JUN_SET,J) AND
                      ( ORD(YR) LE (AVAILABILITY(I,JUN_SET,J)-FIRST_YR) ))=0;

AL.FX(I,JUN_SET,J,YR)$(Not_Prefered(I)and LFT_OPTLINKSO_SET(I,JUN_SET,J) )=0;
*************************************************************************************************************************************

*$OFFTEXT


*******************NAMING THE MODEL AND LISTING THE NUMBER OF EQUATIONS INCLUDED************************
MODEL   AW_EBSD_1 /MIN_DEF,
                   MASS_BALANCE_EQ,
                   MAX_CAPACITY_EQ_NOT_DM,
                   MIN_CAP_EXT_LINK_EQ,
                   MIN_CAP_OPT_NODE_lft_EQ,
                   CAPACITY_EQ_JUST_DM,
                   CONTINUITY_EQ,
                   DEPENDENCY_EQ,
                   EXCLUSIVITY_EQ
                                     / ;
*MIN_COST
MODEL   AW_EBSD_2 /MIN_COST,
                   MASS_BALANCE_EQ,
                   MAX_CAPACITY_EQ_NOT_DM,
                   CAPACITY_EQ_JUST_DM,
                   CONTINUITY_EQ,
                   QQ_EQ,
                   QQQ_EQ,
                   QQ_EQ_LINEARISE,
                   DEPENDENCY_EQ,
                   EXCLUSIVITY_EQ,
                   MIN_CAP_EXT_LINK_EQ,
                   MIN_CAP_OPT_NODE_lft_EQ
                  / ;
*************************************************************************************************************************************
**********SET THE RELATIVE GAP EQUAL TO ZERO (SEE HOW THE BRANCH AND BOUND ALGHORITM WORKS TO UNDERSTAND WHAT THE 'GAP' IS)**********************************************************************************************
        AW_EBSD_1.optcr = 0.0 ;
*************COMMAND TO SOLVE THE MODEL********************************************************************************************
        SOLVE AW_EBSD_1 USING MIP maximizing DEF_TOT;
********ONCE YOU HAVE MAXIMISED THE LEVEL OF DEMADN SATISFACTION (ALPHA) AND BETHA, FIX IT, AND USE IT FOR THE NEXT MODEL****************************
        ALPHA.FX(YR,SCENARIO,I)$DEMAND_APPLY(I)=ALPHA.L(YR,SCENARIO,I)$DEMAND_APPLY(I) ;
        BETHA.FX(YR,SCENARIO,I,JUN_SET,J)$LEX_ONLY_SET(I,JUN_SET,J)=BETHA.L(YR,SCENARIO,I,JUN_SET,J)$LEX_ONLY_SET(I,JUN_SET,J)  ;
*********DISPLAY RESULTS**********************************************************************************************************
        DISPLAY DEF_TOT.L,ALPHA.L,Q.L
        BETHA.L,AL.L   ;
**********************************************************************************************************************************


************************MGA SECTION (GENERATING MULTIPLE SOLUTIONS***************************************************************************************************************************************************
Sets   soln           possible solutions in the solution pool /file1*file1000/
       solnpool(soln) actual solutions;
Scalar cardsoln     number of solutions in the pool;
Alias (soln,s1,s2), (*,u);

Parameters
COSTX(soln,*),DEF_TOTX(soln,*)
QX(soln,I,JUN_SET,J,SCENARIO,YR),QQQX(soln,I,JUN_SET,J,SCENARIO,FF),
QQX(soln,I,JUN_SET,J,SCENARIO,FF), Q_ALX(soln,I,JUN_SET,J,SCENARIO,YR)
ALPHAX(soln,YR,SCENARIO,I)
BETHAX(soln,YR,SCENARIO,I,JUN_SET,J)
ALX(soln,I,JUN_SET,J,YR)
BLX(soln,I,JUN_SET,J);

files fsoln, fcpx / cplex.opt /;
options limrow=100, limcol=100, optcr=0, mip=cplex;
Option SysOut = On;

AW_EBSD_1.optfile=1;AW_EBSD_1.solprint=2;
*solprint keyword controls the printing of the solution report.
AW_EBSD_1.savepoint= 1;
*the solver to write out a basis and then load that basis when doing the solution for the related model.
AW_EBSD_2.optfile=1; AW_EBSD_2.solprint=2;
AW_EBSD_2.savepoint= 1;


* The code to load different solution from gdx files will be used
* several times in this program and we therefore copy it into an include file.
$onecho > readsoln.gms
execute_load 'solnpool.gdx', solnpool=index;
cardsoln = card(solnpool); display cardsoln;

COSTX(soln,u)=0;DEF_TOTX(soln,u)=0;
QX(soln,I,JUN_SET,J,SCENARIO,YR)=0;QQQX(soln,I,JUN_SET,J,SCENARIO,FF)=0;
QQX(soln,I,JUN_SET,J,SCENARIO,FF)=0; Q_ALX(soln,I,JUN_SET,J,SCENARIO,YR)=0 ;
ALPHAX(soln,YR,SCENARIO,I)=0;BETHAX(soln,YR,SCENARIO,I,JUN_SET,J)=0;
ALX(soln,I,JUN_SET,J,YR)=0; BLX(soln,I,JUN_SET,J)=0 ;

*One might want to read in a large number of GDX files
*or execute external programs with altering parameters.
*The put_utility or put_utilities commands in GAMS allow such functions.

loop(solnpool(soln),
    put_utility fsoln 'gdxin' / solnpool.te(soln);
    execute_loadpoint;

  BLX(soln,I,JUN_SET,J)=1$(SUM(YR,AL.l(I,JUN_SET,J,YR)) GE 1)+0$(SUM(YR,AL.l(I,JUN_SET,J,YR)) EQ 0);
  ALX(soln,I,JUN_SET,J,YR)=AL.L(I,JUN_SET,J,YR);
  QX(soln,I,JUN_SET,J,SCENARIO,YR)=Q.L(I,JUN_SET,J,SCENARIO,YR);
  alphaX(soln,YR,SCENARIO,I)=alpha.L(YR,SCENARIO,I);
  BETHAX(soln,YR,SCENARIO,I,JUN_SET,J)=BETHA.l(YR,SCENARIO,I,JUN_SET,J);
  costX(soln,'COST')=COST.l;
  DEF_TOTX(soln,'COST')=DEF_TOT.l;
  QX(soln,I,JUN_SET,J,SCENARIO,YR)=Q.l(I,JUN_SET,J,SCENARIO,YR) ;
  QQQX(soln,I,JUN_SET,J,SCENARIO,FF)=QQQ.l(I,JUN_SET,J,SCENARIO,FF)  ;
  QQX(soln,I,JUN_SET,J,SCENARIO,FF)=QQ.l(I,JUN_SET,J,SCENARIO,FF) ;
  Q_ALX(soln,I,JUN_SET,J,SCENARIO,YR)=Q_AL.l(I,JUN_SET,J,SCENARIO,YR) ;
);
* Restore the solution reported to GAMS
execute_loadpoint 'AW_EBSD_2_p.gdx';
$offecho



*  2. Use the populate procedure instead of regular optimize procedure (option
*     'solnpoolpop 2'). By default we will generate 20 solutions determined by
*     the default of option populatelim. This is a simple model which is quickly
*     solved with heuristics and cuts, so we need to let Cplex retain sufficient
*     exploration space to find alternative solutions. This is done with option
*     'solnpoolintensity 4'. we call the populate procedure, but we want solutions that are
*     that are within 2% of the optimum.
putclose fcpx 'solnpool solnpool.gdx' / 'solnpoolintensity 4' / 'solnpoolpop 2'/ 'solnpoolgap 0.02';
*'populatelim 15000' / 'solnpoolcapacity 250' / 'solnpoolreplace 2' /


        AW_EBSD_2.optcr = 0.0 ;
        SOLVE AW_EBSD_2 USING MIP minimizing COST;
        DISPLAY  COST.L,Q.L,AL.L,Q_AL.L,QQ.L,QQQ.L,
        BETHA.L,ALPHA.L;
*-------------------------------------------------------------------------------
$include readsoln
display costX,BLX;
*****************************************END OF MGA SECTION*************************************

************EXPORT RESULTS IN EXCEL*************************************************************

execute_unload "results_MGA.gdx" costX,QX,BLX,ALX,alphaX,BETHAX,DEF_TOTX
                                 QQQX,QQX,Q_ALX ;

     execute 'gdxxrw.exe results_MGA.gdx par=costX rng=costX! RDIM=1 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=DEF_TOTX rng=DEF_TOTX! RDIM=1 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=alphaX rng=alphaX! RDIM=2 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=BETHAX rng=BETHAX! RDIM=2 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=BLX rng=BLX! RDIM=1 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=ALX rng=ALX! RDIM=4 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=QX rng=QX! RDIM=4 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=QX rng=QX! RDIM=4 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=QQQX rng=QQQX! RDIM=5 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=QQX rng=QQX! RDIM=5 SQ=N'
     execute 'gdxxrw.exe results_MGA.gdx par=Q_ALX rng=Q_ALX! RDIM=5 SQ=N'
 ;


