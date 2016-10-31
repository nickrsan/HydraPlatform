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
