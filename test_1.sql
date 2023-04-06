-- Нужно указать путь к файлу xml

create schema if not exists test;
drop procedure if exists test.parse_xml;

create or replace procedure test.parse_xml()
language plpgsql
as $$
begin
  drop table if exists med_help_data, test.schet, test.sluch, test.usl;
  create extension if not exists "uuid-ossp";
   
  create temp table med_help_data as
  with data as ( 
    select 
    *,
    "xmlparse" as query_column
    from xmlparse(document convert_from(pg_read_binary_file('C:\Users\Public\Documents\C8BAFCEC-B253-4784-A4FA-AE8632F05501.xml'), 'UTF-8')) 
  ), 
  cte_schet as (select 
     xml.*
  from data, xmltable ('/ZL_LIST/SCHET' passing query_column 
              columns code_mo varchar PATH 'CODE_MO',
                  "year" int PATH 'YEAR',
                  "month" int PATH 'MONTH',
                  plat int PATH 'PLAT',
                  "comments" varchar PATH 'COMENTS') xml
  ),

  cte_sluch as (
    select 
    xml.*     
    from data, xmltable ('/ZL_LIST/SCHET/SLUCH' passing query_column 
          columns id_sluch   uuid         path 'ID_SLUCH',
                  pr_nov     int          path 'PR_NOV',
                  vidpom     int          path 'VIDPOM',
                  moddate    timestamp    path 'MODDATE',
                  begdate    timestamp    path 'BEGDATE',
                  enddate    timestamp    path 'ENDDATE',
                  mo_custom  varchar(6)   path 'MO_CUSTOM',
                  lpubase    int          path 'LPUBASE',
                  id_stat    int          path 'ID_STAT',
                  smo        varchar(5)   path 'SMO',
                  smo_ok     varchar(5)   path 'SMO_OK',
                  lpucode    int          path 'LPUCODE',
                  npr_mo     varchar(6)   path 'NPR_MO',
                  npr_type   int          path 'NPR_TYPE',
                  npr_mdcode varchar(8)   path 'NPR_MDCODE',
                  extr       int          path 'EXTR',
                  nhistory   varchar(60)  path 'NHISTORY',
                  rslt       int          path 'RSLT',
                  prvs       int          path 'PRVS',
                  profil     varchar(11)  path 'PROFIL',
                  det        int          path 'DET',
                  iddokt     varchar(8)   path 'IDDOKT',
                  signpay    int          path 'SIGNPAY',
                  idsp       int          path 'IDSP',
                  grp_sk     int          path 'GRP_SK',
                  oplata     int          path 'OPLATA',
                  ed_col     numeric(5,2) path 'ED_COL',
                  p_per      int          path 'P_PER',
                  podr       int          path 'PODR',
                  npr_date   date         path 'NPR_DATE',
                  usl_ok     int          path 'USL_OK',
                  comentsl   varchar(250) path 'COMENTSL',
                  code_mo    varchar(6)   path '../CODE_MO') xml
  ),

  cte_sluch_pd as (
    select 
     xml.* 
    from data, xmltable ('/ZL_LIST/SCHET/SLUCH/PD' passing query_column 
               columns id_sluch   uuid        path '../ID_SLUCH',
                   pdt        int         path 'PDT',
                   enp        varchar(16) path 'ENP',
                   w          int         path 'W',
                   dr         date        path 'DR',
                   vpolis     int         path 'VPOLIS',
                   npolis     varchar(20) path 'NPOLIS',
                   fam        varchar(40) path 'FAM',
                   im         varchar(40) path 'IM',   
                   ot         varchar(40) path 'OT',
                   doctype    varchar(2)  path 'DOCTYPE',
                   docser     varchar(10) path 'DOCSER',
                   docnum     varchar(20) path 'DOCNUM',
                   okatog     varchar(12) path 'OKATOG') xml
  ),

  cte_sluch_ds as (
    select 
    xml.* 
    from data, xmltable ('/ZL_LIST/SCHET/SLUCH/DS' passing query_column 
              columns id_sluch   uuid        path '../ID_SLUCH',
                  ds_way     varchar(10) path 'DS_WAY',
                  ds_in      varchar(10) path 'DS_IN',
                  ds_main    varchar(10) path 'DS_MAIN') xml),

  cte_usl as (
    select 
    xml.* 
    from data, xmltable ('/ZL_LIST/SCHET/SLUCH/USL' passing query_column 
               columns id_usl   uuid         path 'ID_USL',
                   code_usl varchar(16)  path 'CODE_USL',
                   prvs     int          path 'PRVS',
                   dateusl  date         path 'DATEUSL',
                   code_md  varchar(8)   path 'CODE_MD',
                   skind    int          path 'SKIND',
                   typeoper int          path 'TYPEOPER',
                   podr     int          path 'PODR',
                   profil   varchar(11)  path 'PROFIL',
                   det      int          path 'DET',
                   bedprof  int          path 'BEDPROF',
                   kol_usl  numeric(6,2) path 'KOL_USL',
                   id_sluch uuid         path '../ID_SLUCH') xml
  ),

  cte_usl_sumusl as (
    select 
    xml.* 
    from data, xmltable('/ZL_LIST/SCHET/SLUCH/USL/SUMUSL' passing query_column 
               columns tarif  numeric(15,2) path 'TARIF',
                   id_usl uuid path '../ID_USL') xml
  )
    
  select 
    s.code_mo     as code_mo,   
    s."year"      as "year",     
    s."month"     as "month",    
    s.plat        as plat,       
    s."comments"  as "comments",
    sl.id_sluch   as id_sluch,    
    sl.pr_nov     as pr_nov,       
    sl.vidpom     as vidpom,       
    sl.moddate    as moddate,     
    sl.begdate    as begdate,      
    sl.enddate    as enddate,      
    sl.mo_custom  as mo_custom,    
    sl.lpubase    as lpubase,      
    sl.id_stat    as id_stat,      
    sl.smo        as smo,          
    sl.smo_ok     as smo_ok,       
    p.pdt         as pdt,          
    p.enp         as enp,          
    p.w           as w,            
    p.dr          as dr,           
    p.vpolis      as vpolis,      
    p.npolis      as npolis,        
    p.fam         as fam,          
    p.im          as im,           
    p.ot          as ot,           
    p.doctype     as doctype,      
    p.docser      as docser,       
    p.docnum      as docnum,       
    p.okatog      as okatog,       
    sl.lpucode    as lpucode,     
    sl.npr_mo     as npr_mo,       
    sl.npr_type   as npr_type,     
    sl.npr_mdcode as npr_mdcode,   
    sl.extr       as extr,         
    sl.nhistory   as nhistory,     
    d.ds_way      as ds_way,       
    d.ds_in       as ds_in,        
    d.ds_main     as ds_main,      
    sl.rslt       as rslt,       
    sl.prvs       as sluch_prvs,         
    sl.profil     as sluch_profil,       
    sl.det        as sluch_det,          
    sl.iddokt     as iddokt,       
    sl.signpay    as signpay,      
    sl.idsp       as idsp,         
    sl.grp_sk     as grp_sk,       
    sl.oplata     as oplata,       
    sl.ed_col     as ed_col,       
    sl.p_per      as p_per,        
    sl.podr       as podr,         
    sl.npr_date   as npr_date,     
    sl.usl_ok     as usl_ok,       
    sl.comentsl   as comentsl,
    u.id_usl      as id_usl,
    u.code_usl    as code_usl,
    u.prvs        as usl_prvs,
    u.dateusl     as dateusl,
    u.code_md     as code_md,
    u.skind       as skind,
    u.typeoper    as typeoper,
    u.podr        as aspodr,
    u.profil      as usl_profil,
    u.det         as usl_det,
    u.bedprof     as bedprof,
    u.kol_usl     as kol_usl,
    su.tarif      as tarif
  from cte_schet s
    join cte_sluch      sl on sl.code_mo = s.code_mo
    join cte_sluch_pd   p  on p.id_sluch = sl.id_sluch
    join cte_sluch_ds   d  on d.id_sluch = sl.id_sluch
    join cte_usl        u  on u.id_sluch = sl.id_sluch
    join cte_usl_sumusl su on su.id_usl  = u.id_usl;

  create table if not exists test.schet (
    code_mo    varchar(6) primary key,
    "year"     int,
    "month"    int,
    plat       varchar(5),
    "comments" varchar(250)
  );

  create table if not exists test.sluch (
    id_sluch     uuid default uuid_generate_v4 () primary key,
    pr_nov       int,
    vidpom       int,
    moddate      date,
    begdate      date,
    enddate      date,
    mo_custom    varchar(6),
    lpubase      int,
    id_stat      int,
    smo          varchar(5),
    smo_ok       varchar(5),
    pdt          int,
    enp          varchar(16),
    w            int,
    dr           date,
    vpolis       int,
    npolis       varchar(20),
    fam          varchar(40),
    im           varchar(40),   
    ot           varchar(40),
    doctype      varchar(2),
    docser       varchar(10),
    docnum       varchar(20),
    okatog       varchar(12),
    lpucode      int,
    npr_mo       varchar(6),
    npr_type     int,
    npr_mdcode   varchar(8),
    extr         int,
    nhistory     varchar(60),
    ds_way       varchar(10), 
    ds_in        varchar(10),
    ds_main      varchar(10),
    rslt         int,
    prvs         int,
    profil       varchar(11),
    det          int,
    iddokt       varchar(8),
    signpay      int,
    idsp         int,
    grp_sk       int,
    oplata       int,
    ed_col       numeric(5,2),
    p_per        int,
    podr         int,
    npr_date     date,
    usl_ok       int,
    comentsl     varchar(250),
    code_mo      varchar(6),
    foreign key (code_mo) references test.schet(code_mo) on delete cascade
  );

  create table if not exists test.usl (
    id_usl     uuid default uuid_generate_v4 () primary key,
    code_usl   varchar(16),
    prvs       int,
    dateusl    date,
    code_md    varchar(8),
    skind      int,
    typeoper   int,
    podr       int,
    profil     varchar(11),
    det        int,
    bedprof    int,
    kol_usl    numeric(6,2),
    tarif      numeric(15,2),
    id_sluch   uuid,
    foreign key (id_sluch) references test.sluch(id_sluch) on delete cascade
  );

  insert into test.schet (code_mo, "year", "month", plat, "comments")
  select distinct
    code_mo,
    "year",
    "month",
    plat,
    "comments"
  from med_help_data;

  insert into test.sluch (id_sluch, pr_nov, vidpom, moddate, begdate, enddate, mo_custom, lpubase, id_stat, smo, 
    smo_ok, pdt, enp, w, dr, vpolis, npolis, fam, im, ot, doctype, docser, docnum, okatog, lpucode, npr_mo, npr_type, 
    npr_mdcode, extr, nhistory, ds_way, ds_in, ds_main, rslt, prvs, profil, det, iddokt, signpay,
    idsp, grp_sk, oplata, ed_col, p_per, podr, npr_date, usl_ok, comentsl, code_mo)
  select distinct
    id_sluch,    
    pr_nov,      
    vidpom,      
    moddate,     
    begdate,     
    enddate,     
    mo_custom,   
    lpubase,     
    id_stat,     
    smo,        
    smo_ok,      
    pdt,         
    enp,         
    w,           
    dr,          
    vpolis,      
    npolis,      
    fam,         
    im,          
    ot,         
    doctype,     
    docser,      
    docnum,      
    okatog,      
    lpucode,     
    npr_mo,      
    npr_type,    
    npr_mdcode,  
    extr,        
    nhistory,    
    ds_way,      
    ds_in,       
    ds_main,     
    rslt,        
    sluch_prvs,        
    sluch_profil,      
    sluch_det,         
    iddokt,      
    signpay,     
    idsp,        
    grp_sk,      
    oplata,      
    ed_col,      
    p_per,       
    podr,        
    npr_date,    
    usl_ok,       
    comentsl, 
    code_mo  
  from med_help_data;

  insert into test.usl (id_usl, code_usl, prvs, dateusl, code_md, skind, typeoper, podr, profil,
    det, bedprof, kol_usl, tarif, id_sluch)
  select distinct
    id_usl,
    code_usl,
    usl_prvs,
    dateusl,
    code_md,
    skind,
    typeoper,
    podr,
    usl_profil,
    usl_det,
    bedprof,
    kol_usl,
    tarif,
    id_sluch
  from med_help_data;
  
end;$$;

call test.parse_xml();
-- select * from test.schet;
-- select * from test.sluch;
-- select * from test.usl;
